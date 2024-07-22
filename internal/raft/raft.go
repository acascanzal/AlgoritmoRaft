
package raft


import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"raft/internal/comun/rpctimeout"
	"sync"
	"time"
)

const (
	FOLLOWER  = "follower"
	LEADER    = "leader"
	CANDIDATE = "candidate"
)
const (
	// Constante para fijar valor entero no inicializado
	IntNOINICIALIZADO = -1

	//  false deshabilita por completo los logs de depuracion
	// Aseguraros de poner kEnableDebugLogs a false antes de la entrega
	kEnableDebugLogs = true

	// Poner a true para logear a stdout en lugar de a fichero
	kLogToStdout = false

	// Cambiar esto para salida de logs en un directorio diferente
	kLogOutputDir = "./logs_raft/"
)

type TipoOperacion struct {
	Operacion string // La operaciones posibles son "leer" y "escribir"
	Clave     string
	Valor     string // en el caso de la lectura Valor = ""
}

// A medida que el nodo Raft conoce las operaciones de las  entradas de registro
// comprometidas, envía un AplicaOperacion, con cada una de ellas, al canal
// "canalAplicar" (funcion NuevoNodo) de la maquina de estados
type AplicaOperacion struct {
	Indice    int // en la entrada de registro
	Operacion TipoOperacion
}

// Tipo de dato Go que representa un solo nodo (réplica) de raftRequestVote
type NodoRaft struct {
	Mutex sync.Mutex // Mutex para proteger acceso a estado compartido

	// Host:Port de todos los nodos (réplicas) Raft, en mismo orden
	Nodos   []rpctimeout.HostPort
	Yo      int // indice de este nodos en campo array "nodos"
	IdLider int
	// Utilización opcional de estelogger para depuración
	// Cada nodo Raft tiene su propio registro de trazas (logs)
	Logger *log.Logger


	RequestVote chan bool

	AppendEntriesChan chan bool

	FollowerChan chan bool

	CandidateChan chan bool

	LeaderChan chan bool


	AplicarOperacion chan AplicaOperacion // AplicaOperacion es un struct.
	CanalRes chan AplicaOperacion // Canal para devolver el resultado de la operacion.

	Committed chan string

	VotedFor int



	VotosRecibidos int
	CurrentTerm    int

	Rol string

	Log []Entry

	CommitIndex int
	LastApplied int
	NextIndex []int
	MatchIndex []int

}

//Estructura de entrada para Log
type Entry struct{
	Indice int
	Mandato int
	Operacion TipoOperacion
}

// Creacion de un nuevo nodo de eleccion
//
// Tabla de <Direccion IP:puerto> de cada nodo incluido a si mismo.
//
// <Direccion IP:puerto> de este nodo esta en nodos[yo]
//
// Todos los arrays nodos[] de los nodos tienen el mismo orden


// NuevoNodo() debe devolver resultado rápido, por lo que se deberían
// poner en marcha Gorutinas para trabajos de larga duracion
func NuevoNodo(nodos []rpctimeout.HostPort, yo int,
	canalAplicarOperacion chan AplicaOperacion, canalRes chan AplicaOperacion) *NodoRaft {
	nr := &NodoRaft{}
	nr.Nodos = nodos
	nr.Yo = yo
	nr.IdLider = -1
	nr.VotedFor = -1 // -1 indica que aun no he votado.
	nr.CurrentTerm = 0
	nr.VotosRecibidos = 0
	nr.Rol = FOLLOWER
	
	nr.CanalRes = canalRes
	nr.AplicarOperacion = canalAplicarOperacion
	nr.MatchIndex = make([]int, len(nodos))
	nr.NextIndex = make([]int, len(nodos))
	nr.LastApplied = -1
	
	nr.CommitIndex = -1
	nr.Committed = make(chan string)
	
	nr.RequestVote = make(chan bool)
	nr.AppendEntriesChan = make(chan bool)
	nr.FollowerChan = make(chan bool)
	nr.CandidateChan = make(chan bool)
	nr.LeaderChan = make(chan bool)

	if kEnableDebugLogs {
		nombreNodo := nodos[yo].Host() + "_" + nodos[yo].Port()
		logPrefix := fmt.Sprintf("%s", nombreNodo)

		fmt.Println("LogPrefix: ", logPrefix)

		if kLogToStdout {
			nr.Logger = log.New(os.Stdout, nombreNodo+" -->> ",
				log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt",
				kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			nr.Logger = log.New(logOutputFile,
				logPrefix+" -> ", log.Lmicroseconds|log.Lshortfile)
		}
		nr.Logger.Println("logger initialized")
	} else {
		nr.Logger = log.New(ioutil.Discard, "", 0)
	}


	go nr.raftProtocol()

	return nr
}


// Quizas interesante desactivar la salida de depuracion
// de este nodo
func (nr *NodoRaft) para() {
	go func() { time.Sleep(5 * time.Millisecond); os.Exit(0) }()
}


// Primer valor devuelto es el indice de este  nodo Raft el el conjunto de nodos
// la operacion si consigue comprometerse.
// El segundo valor es el mandato en curso
// El tercer valor es true si el nodo cree ser el lider
// Cuarto valor es el lider, es el indice del líder si no es él
func (nr *NodoRaft) obtenerEstado() (int, int, bool, int) {
	var yo int = nr.Yo
	var mandato int = nr.CurrentTerm
	var esLider bool = nr.IdLider == nr.Yo
	var idLider int = nr.IdLider

	return yo, mandato, esLider, idLider
}



func (nr *NodoRaft) obtenerEstadoRegistro() (int,int){
	ind := -1 // En caso de que len(nr.Log) != 0 se devuelve -1
	mandato := 0 // En caso de que len(nr.Log) != 0 se devuelve 0

	if len(nr.Log) != 0 {
		ind = nr.CommitIndex
		mandato = nr.Log[ind].Mandato
	}

	return ind,mandato
}

// El servicio que utilice Raft (base de datos clave/valor, por ejemplo)
// Quiere buscar un acuerdo de posicion en registro para siguiente operacion
// solicitada por cliente.

// Si el nodo no es el lider, devolver falso
// Sino, comenzar la operacion de consenso sobre la operacion y devolver en
// cuanto se consiga
//
// No hay garantia que esta operacion consiga comprometerse en una entrada de
// de registro, dado que el lider puede fallar y la entrada ser reemplazada
// en el futuro.
// Primer valor devuelto es el indice del registro donde se va a colocar
// la operacion si consigue comprometerse.
// El segundo valor es el mandato en curso
// El tercer valor es true si el nodo cree ser el lider
// Cuarto valor es el lider, es el indice del líder si no es él
func (nr *NodoRaft) someterOperacion(operacion TipoOperacion) (int, int,
	bool, int, string) {
	idLider := -1
	valorADevolver := ""
	indice := -1
	mandato := -1
	nr.Mutex.Lock()
	soyLider := nr.Yo == nr.IdLider


	// Si yo soy el lider, tengo el permiso para poder añadir la operación
	// al log.
	if soyLider {
		indice = len(nr.Log)
		mandato = nr.CurrentTerm

		entry := Entry{indice,mandato,operacion}
		nr.Logger.Println("Voy a meter la entrada al log: ", valorADevolver)
		nr.Log = append(nr.Log,entry)
		nr.Logger.Println("Entrada metida: ", valorADevolver)
		nr.Mutex.Unlock()
		
		
		
		idLider = nr.Yo
		valorADevolver =  <- nr.Committed
		nr.Logger.Println("Ahora el valor a devolver es: ", valorADevolver)
	} else {
		nr.Mutex.Unlock()
		idLider = nr.IdLider
	}

	return indice, mandato, soyLider, idLider, valorADevolver
}

// -----------------------------------------------------------------------
// LLAMADAS RPC al API
//
// Si no tenemos argumentos o respuesta estructura vacia (tamaño cero)
type Vacio struct{}

func (nr *NodoRaft) ParaNodo(args Vacio, reply *Vacio) error {
	defer nr.para()
	return nil
}

type EstadoParcial struct {
	Mandato int
	EsLider bool
	IdLider int
}

type EstadoRemoto struct {
	IdNodo int
	EstadoParcial
}

func (nr *NodoRaft) ObtenerEstadoNodo(args Vacio, reply *EstadoRemoto) error {
	nr.Mutex.Lock()
	reply.IdNodo, reply.Mandato, reply.EsLider,
		reply.IdLider = nr.obtenerEstado()
	nr.Mutex.Unlock()
	return nil
}

type ResultadoRemoto struct {
	ValorADevolver string
	IndiceRegistro int
	EstadoParcial
}

func (nr *NodoRaft) SometerOperacionRaft(operacion TipoOperacion,
	reply *ResultadoRemoto) error {
	// No es necesario hacer mutex porque someterOperacion ya lo hace.
	nr.Logger.Println("Recibida llamada SometerOperacionRaft(operacion:{ Operacion= ", operacion.Operacion, " Clave= ", operacion.Clave, " Valor= ", operacion.Valor, " })")
	reply.IndiceRegistro, reply.Mandato, reply.EsLider,
		reply.IdLider, reply.ValorADevolver = nr.someterOperacion(operacion)
	return nil
}

type EstadoRegistro struct {
	Indice int
	Mandato int

}

//Llamada RPC para obtener el estado del Log
func (nr *NodoRaft) ObtenerEstadoRegistro(args Vacio, reply *EstadoRegistro) error {
	// nr.Mux.Lock()
	reply.Indice, reply.Mandato = nr.obtenerEstadoRegistro()
	// nr.Mux.Unlock()
	return nil
}

// -----------------------------------------------------------------------
// LLAMADAS RPC protocolo RAFT
//
// Structura de ejemplo de argumentos de RPC PedirVoto.
//
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
type ArgsPeticionVoto struct {
	Term        	 int // Mandato del candidato.
	CandidateId 	 int // Id del candidato.
	LastLogIndex	 int // Indice del último Entry del Log candidato.
	LastLogTerm		 int // Mandato de la última Entry del Log candidato.
}

// Structura de ejemplo de respuesta de RPC PedirVoto,
//
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
type RespuestaPeticionVoto struct {
	Term        int
	VoteGranted bool
}

// Método para mandar los RPC
func requestVotes(nr *NodoRaft) {
	var reply RespuestaPeticionVoto
	for i := 0; i < len(nr.Nodos); i++ {
		if i != nr.Yo {
			if len(nr.Log) != 0 {
				lastLogIndex := len(nr.Log) - 1
				lastLogTerm := nr.Log[lastLogIndex].Mandato
				nr.Logger.Println("Enviando peticion de voto a nodo ", i, " con mandato ", nr.CurrentTerm, " y entries ", lastLogIndex, " ", lastLogTerm)
				go nr.enviarPeticionVoto(i, &ArgsPeticionVoto{nr.CurrentTerm,
					nr.Yo,lastLogIndex,lastLogTerm}, &reply)
			}else{
				nr.Logger.Println("Enviando peticion de voto a nodo ", i, " con mandato ", nr.CurrentTerm, " y entries ", -1, " ", 0)
				go nr.enviarPeticionVoto(i, &ArgsPeticionVoto{nr.CurrentTerm,
					nr.Yo,-1,0}, &reply)
			}
			
		}
	}
}

// Metodo para RPC PedirVoto
func (nr *NodoRaft) PedirVoto(peticion *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) error {
		nr.Mutex.Lock()

		if peticion.Term < nr.CurrentTerm {
			reply.VoteGranted = false
			reply.Term = nr.CurrentTerm
		} else if peticion.Term > nr.CurrentTerm {

			if (len(nr.Log) == 0) || puedeSerLider(nr,peticion.LastLogIndex,peticion.LastLogTerm) {
				// El mandato que me han mandado cumple las condiciones 
				// necesarias para ser lider o la longitud de mi log es 0
				// le voy a darle mi voto.
				reply.VoteGranted = true
				nr.VotedFor = peticion.CandidateId
				nr.CurrentTerm = peticion.Term
			} else {
				reply.VoteGranted = false
				nr.CurrentTerm = peticion.Term
	
			}
			reply.Term = nr.CurrentTerm
			if nr.Rol == LEADER || nr.Rol == CANDIDATE {
				nr.FollowerChan <- true
			}
		} else if peticion.Term == nr.CurrentTerm && peticion.CandidateId != nr.VotedFor {
			reply.Term = nr.CurrentTerm
			reply.VoteGranted = false
			
			
		}
	nr.Mutex.Unlock()
	return nil // Todo funciona correctamente.
}

type ArgAppendEntries struct {
	Term     int
	LeaderId int
	PrevLogIndex 		int   // Indice de la Entry que precede a las nuevas
	PrevLogTerm 		int   // Mandato de la Entry de PrevLogIndex
	Entries 			Entry // Entrada que guardo en el log.
	LeaderCommit 		int   // Último indice comprometido.
}

type Results struct {
	Term    int
	Success bool
}

// Metodo de tratamiento de llamadas RPC AppendEntries
func (nr *NodoRaft) AppendEntries(args *ArgAppendEntries,
	results *Results) error {
	nr.Mutex.Lock()
	if args.Term < nr.CurrentTerm {
		results.Term = nr.CurrentTerm
		results.Success = false
	// Si el mandato que me mandan es mayor, tengo que actualizar el de todos.
	} else if args.Term > nr.CurrentTerm {
		
		nr.IdLider = args.LeaderId
		nr.CurrentTerm = args.Term
		results.Term = nr.CurrentTerm

		if nr.Rol == LEADER || nr.Rol == CANDIDATE {
			nr.FollowerChan <- true
		} else {
			if(args.LeaderCommit > nr.CommitIndex){
				nr.CommitIndex = min(args.LeaderCommit, len(nr.Log)-1)
			}
			nr.AppendEntriesChan <- true
		}
	} else { // Caso en el que los mandatos son iguales. Caso ideal.
		// Compruebo los logs.
		nr.IdLider = args.LeaderId
		results.Term = args.Term
		if len(nr.Log) == 0 {
			// Añado la nueva entrada siempre que no sea vacia.
			if args.Entries != (Entry{}) {
				nr.Log = append(nr.Log, args.Entries)
			}
			results.Success = true
		} else if !logConsistente(nr, args.PrevLogIndex, args.PrevLogTerm) {
			// El Log no es consistente, entonces rechazo nuevas entradas.
			results.Success = false
		} else {
			// El Log es consistente y tengo entradas en mi Log.
			// Tengo que eliminar las entradas posteriores al PrevLogIndex
			if args.Entries != (Entry{}) {
				// El slice es hasta uno menos, por eso hay PrevLogIndex+1
				nr.Log = nr.Log[0 : args.PrevLogIndex+1]
				nr.Log = append(nr.Log, args.Entries)
			}
			results.Success = true
		}
		if args.LeaderCommit > nr.CommitIndex {
			nr.CommitIndex = min(args.LeaderCommit, len(nr.Log)-1)
		}
		nr.AppendEntriesChan <- true
	}
	nr.Mutex.Unlock()
	return nil
}

// ----- Metodos/Funciones a utilizar como clientes
//
//

func (nr *NodoRaft) enviarPeticionVoto(nodo int, args *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) bool {
	// Pido el voto a los demas servidores, mandando mi request
	// y un TimeOut de espera.
	err := nr.Nodos[nodo].CallTimeout("NodoRaft.PedirVoto", args, reply,
		20*time.Millisecond)
	if err != nil {
		return false
	} else {
		nr.Logger.Println("Respuesta de nodo ", nodo, " a la petición de voto: ", reply)
		if reply.Term > nr.CurrentTerm {
			nr.CurrentTerm = reply.Term
			nr.FollowerChan <- true

		}else if reply.VoteGranted {
				nr.Mutex.Lock()
				nr.VotosRecibidos++ // Debe ser atomico.
				nr.Mutex.Unlock()
				if nr.VotosRecibidos > len(nr.Nodos)/2 {
					nr.LeaderChan <- true
				}
				// El mandato que me mandan es mayor que el mio.
		}
		
	}
	return true
}

func (nr *NodoRaft) mandarHeartbeat(nodo int, args *ArgAppendEntries,results *Results) bool {
	err := nr.Nodos[nodo].CallTimeout("NodoRaft.AppendEntries", args, results,10*time.Millisecond)
 	if err != nil {
 		return false
 	} else {
 		if results.Term > nr.CurrentTerm {

 		nr.Mutex.Lock()
		nr.CurrentTerm = results.Term
		nr.IdLider = -1
		nr.FollowerChan  <- true
		nr.Mutex.Unlock()
 	}
 	return true
 	}
}


func (nr *NodoRaft) nuevaEntrada(nodo int, args *ArgAppendEntries, results *Results) bool {
	err := nr.Nodos[nodo].CallTimeout("NodoRaft.AppendEntries", args, results, 20*time.Millisecond)
	if err != nil {
		return false
		
	} else {
		if results.Success{
			
			nr.MatchIndex[nodo] = nr.NextIndex[nodo]
			nr.NextIndex[nodo] ++
			nr.Mutex.Lock()
			if nr.MatchIndex[nodo] > nr.CommitIndex{
				nr.VotosRecibidos++
				if nr.VotosRecibidos >= len(nr.Nodos)/2{
					nr.CommitIndex++
					nr.VotosRecibidos = 0
					
				}
			}
			nr.Mutex.Unlock()
		} else {
			nr.NextIndex[nodo]--
		}
		return true
	}
}


func (nr *NodoRaft) raftProtocol() {
	for {
		for nr.Rol == FOLLOWER {
			nr.Logger.Println("Soy follower")
			select {
			case <-nr.AppendEntriesChan: // Me bloqueo hasta recibir un
				// mensaje por el canal.
				nr.Rol = FOLLOWER // Me convierto en FOLLOWER.
			case <-time.After(getRandomTimeout()): // Pasa mi TimeOut.
				nr.IdLider = -1
				nr.Rol = CANDIDATE // Me convierto en candidato.
			}
		}

		for nr.Rol == CANDIDATE {
			nr.Logger.Println("Soy un candidate")
			
			if nr.CommitIndex > nr.LastApplied {
				nr.LastApplied++
				operacion := AplicaOperacion{nr.LastApplied, nr.Log[nr.LastApplied].Operacion}
				nr.AplicarOperacion <- operacion
			}
			nr.CurrentTerm++
			nr.VotedFor = nr.Yo
			nr.VotosRecibidos = 1
			requestVotes(nr)

			select {
			case <-nr.AppendEntriesChan: //Ha llegado un heartbeat
				nr.Rol = FOLLOWER
			case <-nr.FollowerChan:
				nr.Rol = FOLLOWER
			//Timeout, nueva eleccion
			case <-time.After(getRandomTimeout()):
				nr.Rol = CANDIDATE
			case <-nr.LeaderChan:
				for i := 0; i < len(nr.Nodos); i++ {
					if i != nr.Yo {
						nr.NextIndex[i] = len(nr.Log)
						nr.MatchIndex[i] = -1
					}
				
				}

				nr.Rol = LEADER
			}
		}

		for nr.Rol == LEADER {
			nr.Logger.Println("Soy un leader")
			nr.IdLider = nr.Yo
			sendAppendEntries(nr) //Mandar heartbeat a todos los nodos
			select {
			case <-nr.FollowerChan:
				nr.Rol = FOLLOWER

			case <-time.After(50 * time.Millisecond): //pasado el timeout
				// mando heartbeat
				nr.Logger.Println("TimeOut leader")
				if nr.CommitIndex > nr.LastApplied {
					nr.LastApplied++
					operacion := AplicaOperacion {nr.LastApplied, nr.Log[nr.LastApplied].Operacion}
					nr.Logger.Println("Operacion a aplicar: ", operacion)
					nr.AplicarOperacion <- operacion
					operacion = <- nr.CanalRes
					nr.Logger.Println("Operacion a aplicada: ", operacion)
					nr.Committed <- operacion.Operacion.Valor
				}
				nr.Rol = LEADER
			}
		}
	}
}

// Devuelve un timeout aleatorio entre 150 y 300 ms.
func getRandomTimeout() time.Duration {
	return time.Duration(200+rand.Intn(400)) * time.Millisecond
}


func puedeSerLider(nr *NodoRaft, lastLogIndex int, lastLogTerm int) bool {
	esMejor := false
	if lastLogTerm > nr.Log[len(nr.Log)-1].Mandato {
		esMejor = true
	}
	
	if lastLogTerm == nr.Log[len(nr.Log)-1].Mandato && lastLogIndex >= len(nr.Log)-1{
		esMejor = true
	}
	return esMejor
}

func logConsistente(nr *NodoRaft, prevLogIndex int, prevLogTerm int) bool {
	if prevLogIndex > len(nr.Log)-1{
		return false
	} else if nr.Log[prevLogIndex].Mandato != prevLogTerm {
		return false
	}else{
		return true
	}
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
// Función que se encarga de los ApendEntries, decide si hay que enviar
// un ApendEntries o un Heartbeat.
func sendAppendEntries(nr *NodoRaft) {
	var results Results
	for i:= 0; i < len(nr.Nodos) ; i++ {
		if i != nr.Yo {
			// Hay nuevas entradas en el log, por lo que hay que enviarlas.
			if len(nr.Log)-1 >= nr.NextIndex[i] {
				entry := Entry {nr.NextIndex[i], nr.Log[nr.NextIndex[i]].Mandato,
							nr.Log[nr.NextIndex[i]].Operacion}
				if nr.NextIndex[i] != 0 {
					prevLogIndex := nr.NextIndex[i] - 1
					prevLogTerm := nr.Log[prevLogIndex].Mandato
					go nr.nuevaEntrada(i, &ArgAppendEntries{nr.CurrentTerm, 
						nr.Yo,prevLogIndex,prevLogTerm,entry,nr.CommitIndex,}, 
						&results)
				} else {
					go nr.nuevaEntrada(i, &ArgAppendEntries{nr.CurrentTerm, nr.Yo,
						-1, 0, entry,		nr.CommitIndex,
					}, &results)
				}
			} else { // No hay nuevas entradas en el log, mando un Hearbeat.
				if nr.NextIndex[i] != 0 {
					prevLogIndex := nr.NextIndex[i] - 1
					prevLogTerm := nr.Log[prevLogIndex].Mandato
					// Envio un Entry vacio.
					go nr.mandarHeartbeat(i, &ArgAppendEntries{nr.CurrentTerm,
						nr.Yo, prevLogIndex, prevLogTerm, Entry{}, nr.CommitIndex},
						&results)
				} else {
					go nr.mandarHeartbeat(i, &ArgAppendEntries{nr.CurrentTerm,
						nr.Yo, -1, 0, Entry{}, nr.CommitIndex}, &results)
				}
			}
		}
	}
}

