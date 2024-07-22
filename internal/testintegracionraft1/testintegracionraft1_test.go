package testintegracionraft1

import (
	"fmt"
	"raft/internal/comun/check"

	//"log"
	//"crypto/rand"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"raft/internal/comun/rpctimeout"
	"raft/internal/despliegue"
	"raft/internal/raft"
)

const (
	//hosts
	MAQUINA1 = "127.0.0.1"
	MAQUINA2 = "127.0.0.1"
	MAQUINA3 = "127.0.0.1"

	//puertos
	PUERTOREPLICA1 = "29001"
	PUERTOREPLICA2 = "29002"
	PUERTOREPLICA3 = "29003"

	//nodos replicas
	REPLICA1 = MAQUINA1 + ":" + PUERTOREPLICA1
	REPLICA2 = MAQUINA2 + ":" + PUERTOREPLICA2
	REPLICA3 = MAQUINA3 + ":" + PUERTOREPLICA3

	// paquete main de ejecutables relativos a PATH previo
	EXECREPLICA = "cmd/srvraft/main.go"

	// comandos completo a ejecutar en máquinas remota con ssh. Ejemplo :
	// 				cd $HOME/raft; go run cmd/srvraft/main.go 127.0.0.1:29001

	// Ubicar, en esta constante, nombre de fichero de vuestra clave privada local
	// emparejada con la clave pública en authorized_keys de máquinas remotas

	PRIVKEYFILE = "id_rsa"
)

// PATH de los ejecutables de modulo golang de servicio Raft


var PATH string = filepath.Join(os.Getenv("HOME"), "Escritorio", "distribuidos", 
"Practicas-Sistemas-Distribuidos", "Practica4", 
"raft")

// go run cmd/srvraft/main.go 0 127.0.0.1:29001 127.0.0.1:29002 127.0.0.1:29003
var EXECREPLICACMD string = "cd " + PATH + "; go run " + EXECREPLICA

// TEST primer rango
func TestPrimerasPruebas(t *testing.T) { // (m *testing.M) {
	// <setup code>
	// Crear canal de resultados de ejecuciones ssh en maquinas remotas
	cfg := makeCfgDespliegue(t,
		3,
		[]string{REPLICA1, REPLICA2, REPLICA3},
		[]bool{true, true, true})

	// tear down code
	// eliminar procesos en máquinas remotas
	defer cfg.stop()

	// Run test sequence
	cfg.startDistributedProcesses()
	// Test1 : No debería haber ningun primario, si SV no ha recibido aún latidos
	t.Run("T1:soloArranqueYparada",
		func(t *testing.T) { cfg.soloArranqueYparadaTest1(t) })

	// Test2 : No debería haber ningun primario, si SV no ha recibido aún latidos
	t.Run("T2:ElegirPrimerLider",
		func(t *testing.T) { cfg.elegirPrimerLiderTest2(t) })

	// // Test3: tenemos el primer primario correcto
	t.Run("T3:FalloAnteriorElegirNuevoLider",
		func(t *testing.T) { cfg.falloAnteriorElegirNuevoLiderTest3(t) })

	// Test4: Tres operaciones comprometidas en configuración estable
	t.Run("T4:tresOperacionesComprometidasEstable",
	 	func(t *testing.T) { cfg.tresOperacionesComprometidasEstable(t) })

}

// TEST primer rango
func TestAcuerdosConFallos(t *testing.T) { // (m *testing.M) {
	// <setup code>
	// Crear canal de resultados de ejecuciones ssh en maquinas remotas
	cfg := makeCfgDespliegue(t,
		3,
		[]string{REPLICA1, REPLICA2, REPLICA3},
		[]bool{true, true, true})

	// tear down code
	// eliminar procesos en máquinas remotas
	defer cfg.stop()

	// Test5: Se consigue acuerdo a pesar de desconexiones de seguidor
	t.Run("T5:AcuerdoAPesarDeDesconexionesDeSeguidor ",
		func(t *testing.T) { cfg.AcuerdoApesarDeSeguidor(t) })

	t.Run("T6:SinAcuerdoPorFallos ",
		func(t *testing.T) { cfg.SinAcuerdoPorFallos(t) })

	t.Run("T7:SometerConcurrentementeOperaciones ",
		func(t *testing.T) { cfg.SometerConcurrentementeOperaciones(t) })

}

// ---------------------------------------------------------------------
//
// Canal de resultados de ejecución de comandos ssh remotos
type canalResultados chan string

func (cr canalResultados) stop() {
	close(cr)

	// Leer las salidas obtenidos de los comandos ssh ejecutados
	for s := range cr {
		fmt.Println(s)
	}
}

// ---------------------------------------------------------------------
// Operativa en configuracion de despliegue y pruebas asociadas
type configDespliegue struct {
	t           *testing.T
	conectados  []bool
	numReplicas int
	nodosRaft   []rpctimeout.HostPort
	idLider 	int
	cr          canalResultados
}

// Crear una configuracion de despliegue
func makeCfgDespliegue(t *testing.T, n int, nodosraft []string,
	conectados []bool) *configDespliegue {
	cfg := &configDespliegue{}
	cfg.t = t
	cfg.conectados = conectados
	cfg.numReplicas = n
	cfg.nodosRaft = rpctimeout.StringArrayToHostPortArray(nodosraft)
	cfg.idLider = 0
	cfg.cr = make(canalResultados, 2000)

	return cfg
}

func (cfg *configDespliegue) stop() {
	//cfg.stopDistributedProcesses()

	time.Sleep(50 * time.Millisecond)

	cfg.cr.stop()
}

// --------------------------------------------------------------------------
// FUNCIONES DE SUBTESTS

// Se pone en marcha una replica ?? - 3 NODOS RAFT
func (cfg *configDespliegue) soloArranqueYparadaTest1(t *testing.T) {
	//t.Skip("SKIPPED soloArranqueYparadaTest1")

	fmt.Println(t.Name(), ".....................")

	cfg.t = t // Actualizar la estructura de datos de tests para errores

	// Poner en marcha replicas en remoto con un tiempo de espera incluido
	cfg.startDistributedProcesses()

	// Comprobar estado replica 0
	cfg.comprobarEstadoRemoto(0, 0, false, -1)

	// Comprobar estado replica 1
	cfg.comprobarEstadoRemoto(1, 0, false, -1)

	// Comprobar estado replica 2
	cfg.comprobarEstadoRemoto(2, 0, false, -1)

	// Parar réplicas almacenamiento en remoto
	cfg.stopDistributedProcesses()

	fmt.Println(".............", t.Name(), "Superado")
}

// Primer lider en marcha - 3 NODOS RAFT
func (cfg *configDespliegue) elegirPrimerLiderTest2(t *testing.T) {
	//t.Skip("SKIPPED ElegirPrimerLiderTest2")

	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	// Se ha elegido lider ?
	fmt.Printf("Probando lider en curso\n")
	cfg.pruebaUnLider(3)

	// Parar réplicas alamcenamiento en remoto
	cfg.stopDistributedProcesses() // Parametros

	fmt.Println(".............", t.Name(), "Superado")
}

// Fallo de un primer lider y reeleccion de uno nuevo - 3 NODOS RAFT
func (cfg *configDespliegue) falloAnteriorElegirNuevoLiderTest3(t *testing.T) {
	//t.Skip("SKIPPED FalloAnteriorElegirNuevoLiderTest3")

	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	fmt.Printf("Lider inicial\n")
	IDLider := cfg.pruebaUnLider(3)

	// Desconectar lider
	// ???

	cfg.pararLider(IDLider)
	fmt.Printf("Lider parado\n")

	fmt.Printf("Comprobar nuevo lider\n")
	cfg.pruebaUnLider(3) // Ahora solo tenemos 2 replicas.

	// Parar réplicas almacenamiento en remoto
	cfg.stopDistributedProcesses() //parametros

	fmt.Println(".............", t.Name(), "Superado")
}

// 3 operaciones comprometidas con situacion estable y sin fallos - 3 NODOS RAFT
func (cfg *configDespliegue) tresOperacionesComprometidasEstable(t *testing.T) {
	//t.Skip("SKIPPED tresOperacionesComprometidasEstable")

 cfg.startDistributedProcesses()

 fmt.Printf("Probando líder en curso\n")
 cfg.idLider = cfg.pruebaUnLider(3)
 cfg.comprobarOperacion(0, "escribir", "x", "3", "ok")
 cfg.comprobarOperacion(1, "escribir", "y", "1", "ok")
 cfg.comprobarOperacion(2, "leer", "x", "", "3")
 cfg.stopDistributedProcesses()
 fmt.Println(".............", t.Name(), "Superado")
}

// Se consigue acuerdo a pesar de desconexiones de seguidor -- 3 NODOS RAFT
func (cfg *configDespliegue) AcuerdoApesarDeSeguidor(t *testing.T) {
	//t.Skip("SKIPPED AcuerdoApesarDeSeguidor")

	fmt.Println(t.Name(), ".....................")
	cfg.startDistributedProcesses()
	fmt.Printf("Probando líder en curso\n")
	cfg.idLider = cfg.pruebaUnLider(3)
	cfg.comprobarOperacion(0, "escribir", "x", "3", "ok")
	cfg.pararUnNodo()
	time.Sleep(2 * time.Second)
	fmt.Printf("Comprobando líder actual\n")
	cfg.idLider = cfg.pruebaUnLider(3) 
	cfg.comprobarOperacion(1, "escribir", "y", "1", "ok")
	cfg.comprobarOperacion(2, "escribir", "y", "9", "ok")
	cfg.comprobarOperacion(3, "leer", "y", "", "9")
	cfg.restartDisconnectedProcesses()
	cfg.comprobarOperacion(4, "escribir", "z", "4", "ok")
	cfg.comprobarOperacion(5, "leer", "x", "", "3")
	cfg.comprobarOperacion(6, "escribir", "x", "5", "ok")
	cfg.stopDistributedProcesses()
	fmt.Println(".............", t.Name(), "Superado")
}

// NO se consigue acuerdo al desconectarse mayoría de seguidores -- 3 NODOS RAFT
func (cfg *configDespliegue) SinAcuerdoPorFallos(t *testing.T) {
	//t.Skip("SKIPPED SinAcuerdoPorFallos")


	  fmt.Println(t.Name(), ".....................")
	  cfg.startDistributedProcesses()

	  fmt.Printf("Probando líder en curso\n")
	  cfg.idLider = cfg.pruebaUnLider(3)
	  cfg.comprobarOperacion(0, "escribir", "x", "3", "ok")

	  cfg.pararFollowers()

	  cfg.someterOperacionFallo("escribir", "y", "1")
	  cfg.someterOperacionFallo("escribir", "y", "9")
	  cfg.someterOperacionFallo("leer", "y", "")

	  cfg.restartDisconnectedProcesses()
	  cfg.comprobarOperacion(4, "escribir", "z", "4", "ok")
	  cfg.comprobarOperacion(5, "leer", "x", "", "3")
	  cfg.comprobarOperacion(6, "escribir", "x", "5", "ok")
	  cfg.stopDistributedProcesses()
	  fmt.Println(".............", t.Name(), "Superado")
}

// Se somete 5 operaciones de forma concurrente -- 3 NODOS RAFT
func (cfg *configDespliegue) SometerConcurrentementeOperaciones(t *testing.T) {
	//t.Skip("SKIPPED SometerConcurrentementeOperaciones")

	  fmt.Println(t.Name(), ".....................")
	  cfg.startDistributedProcesses()
	  fmt.Printf("Probando líder en curso\n")
	  cfg.idLider = cfg.pruebaUnLider(3)
	  cfg.someterOperacion("escribir", "x", "3")
	  go cfg.someterOperacion("escribir", "y", "1")
	  go cfg.someterOperacion("escribir", "y", "9")
	  go cfg.someterOperacion("leer", "y", "")
	  go cfg.someterOperacion("escribir", "z", "4")
	  go cfg.someterOperacion("leer", "y", "")
	  time.Sleep(5 * time.Second)
	  cfg.comprobarEstadoRegistro(5)
	  cfg.stopDistributedProcesses()
	  fmt.Println(".............", t.Name(), "Superado")
}

// --------------------------------------------------------------------------
// FUNCIONES DE APOYO
// Comprobar que hay un solo lider
// probar varias veces si se necesitan reelecciones
func (cfg *configDespliegue) pruebaUnLider(numreplicas int) int {
	for iters := 0; iters < 10; iters++ {
		time.Sleep(500 * time.Millisecond)
		mapaLideres := make(map[int][]int)
		for i := 0; i < numreplicas; i++ {
			if cfg.conectados[i] {
				if _, mandato, eslider, _ := cfg.obtenerEstadoRemoto(i); eslider {
					mapaLideres[mandato] = append(mapaLideres[mandato], i)
				}
			}
		}

		ultimoMandatoConLider := -1
		for mandato, lideres := range mapaLideres {
			if len(lideres) > 1 {
				cfg.t.Fatalf("mandato %d tiene %d (>1) lideres",
					mandato, len(lideres))
			}
			if mandato > ultimoMandatoConLider {
				ultimoMandatoConLider = mandato
			}
		}

		if len(mapaLideres) != 0 {

			return mapaLideres[ultimoMandatoConLider][0] // Termina

		}
	}
	cfg.t.Fatalf("un lider esperado, ninguno obtenido")

	return -1 // Termina
}

func (cfg *configDespliegue) obtenerEstadoRemoto(
	indiceNodo int) (int, int, bool, int) {
	var reply raft.EstadoRemoto
	err := cfg.nodosRaft[indiceNodo].CallTimeout("NodoRaft.ObtenerEstadoNodo",
		raft.Vacio{}, &reply, 10*time.Millisecond)


	check.CheckError(err, "Error en llamada RPC ObtenerEstadoRemoto")

	return reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider
}

// start  gestor de vistas; mapa de replicas y maquinas donde ubicarlos;
// y lista clientes (host:puerto)
func (cfg *configDespliegue) startDistributedProcesses() {
	//cfg.t.Log("Before starting following distributed processes: ", cfg.nodosRaft)

	for i, endPoint := range cfg.nodosRaft {
		despliegue.ExecMutipleHosts(EXECREPLICACMD+
			" "+strconv.Itoa(i)+" "+
			rpctimeout.HostPortArrayToString(cfg.nodosRaft),
			[]string{endPoint.Host()}, cfg.cr, PRIVKEYFILE)
			fmt.Println("Arrancando nodo", i)
			cfg.conectados[i] = true

		// dar tiempo para se establezcan las replicas
		time.Sleep(500 * time.Millisecond)
	}
	// aproximadamente 500 ms para cada arranque por ssh en portatil
	time.Sleep(2500 * time.Millisecond)
}

func (cfg *configDespliegue) stopDistributedProcesses() {
	var reply raft.Vacio

	for i := 0; i < len(cfg.nodosRaft); i++ {
		if cfg.conectados[i] == true {
			err := cfg.nodosRaft[i].CallTimeout("NodoRaft.ParaNodo",
				raft.Vacio{}, &reply, 10*time.Millisecond)
			fmt.Println("Parando nodo ", i)
			check.CheckError(err, "Error en llamada RPC Para nodo ")
		}
	}
}

// Comprobar estado remoto de un nodo con respecto a un estado prefijado
func (cfg *configDespliegue) comprobarEstadoRemoto(idNodoDeseado int,
	mandatoDeseado int, esLiderDeseado bool, IdLiderDeseado int) {
	idNodo, _, _, _ := cfg.obtenerEstadoRemoto(idNodoDeseado)

	//cfg.t.Log("Estado replica 0: ", idNodo, mandato, esLider, idLider, "\n")

	if idNodo != idNodoDeseado {
		cfg.t.Fatalf("Estado incorrecto en replica %d en subtest %s",
			idNodoDeseado, cfg.t.Name())
	}

}

func (cfg *configDespliegue) pararLider(lider int) {
	var vacio raft.Vacio
	_ = cfg.nodosRaft[lider].CallTimeout("NodoRaft.ParaNodo", raft.Vacio{}, &vacio, 150*time.Millisecond)

	cfg.conectados[lider] = false
}


 func (cfg *configDespliegue) comprobarOperacion(indiceLog int,
	operacion string, clave string, valor string, valorDevuelto string) {
	
		

	indice, _, _, _, valorADevolver :=  cfg.someterOperacion(operacion,
		clave, valor)
	if indice != indiceLog || valorADevolver != valorDevuelto {
		cfg.t.Fatalf("Operación no sometida correctamente en índice %d en subtest %s",indiceLog, cfg.t.Name())
	}
	fmt.Printf("Acuerdo conseguido\n")
 }

 func (cfg *configDespliegue) someterOperacion(operacion string, clave string,
	valor string) (int, int, bool, int, string) {
	var reply raft.ResultadoRemoto
	op := raft.TipoOperacion{Operacion: operacion, Clave: clave, Valor: valor}
	fmt.Printf("Someter operación a %d\n", cfg.idLider)
	


	err := cfg.nodosRaft[cfg.idLider].CallTimeout("NodoRaft.SometerOperacionRaft",
	op, &reply, 5000*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC SometerOperacionRaft")
	cfg.idLider = reply.IdLider
	return reply.IndiceRegistro, reply.Mandato, reply.EsLider, reply.IdLider,reply.ValorADevolver
}


func (cfg *configDespliegue) pararUnNodo() {
	node := rand.Intn(3)
	for node == cfg.idLider {
		node = rand.Intn(3)
	}

	var reply raft.Vacio
	err := cfg.nodosRaft[node].CallTimeout("NodoRaft.ParaNodo",
		raft.Vacio{}, &reply, 150*time.Millisecond)
	for err != nil {
		err = cfg.nodosRaft[node].CallTimeout("NodoRaft.ParaNodo",
			raft.Vacio{}, &reply, 10*time.Millisecond)
	}

	check.CheckError(err, "Error en llamada RPC ParaNodo")
	cfg.conectados[node] = false
	fmt.Printf("Parado el nodo %d\n", node)
}

func (cfg *configDespliegue) restartDisconnectedProcesses(){
	for i, endPoint := range cfg.nodosRaft {
		if !cfg.conectados[i] {
			despliegue.ExecMutipleHosts(EXECREPLICACMD+
				" "+strconv.Itoa(i)+" "+
				rpctimeout.HostPortArrayToString(cfg.nodosRaft),
				[]string{endPoint.Host()}, cfg.cr, PRIVKEYFILE)
				fmt.Println("Arrancando nodo", i)
			cfg.conectados[i] = true
			fmt.Printf("Arrancado el nodo %d\n", i)
			time.Sleep(500 * time.Millisecond)
		}
		
	}
	time.Sleep(2500 * time.Millisecond)
}

func (cfg *configDespliegue) pararFollowers(){
	var reply raft.Vacio
	for i, endPoint := range cfg.nodosRaft {
		if cfg.conectados[i] && i != cfg.idLider {
			err := endPoint.CallTimeout("NodoRaft.ParaNodo",
				raft.Vacio{}, &reply, 150*time.Millisecond)
			check.CheckError(err, "Error en llamada RPC ParaNodo")
			cfg.conectados[i] = false
			fmt.Printf("Parado el nodo %d\n", i)
		}
	}
}

func (cfg *configDespliegue) someterOperacionFallo(operacion string, clave string, valor string) {
	var reply raft.ResultadoRemoto
	err := cfg.nodosRaft[cfg.idLider].CallTimeout("NodoRaft.SometerOperacionRaft",
		raft.TipoOperacion{operacion, clave, valor}, &reply, 3000*time.Millisecond)
	if err == nil {
		cfg.t.Fatalf("Se ha conseguido acuerdo sin mayoría simple en subtest %s",cfg.t.Name())
	} else {
		fmt.Printf("Acuerdo no conseguido\n")
	}
}

func (cfg *configDespliegue) comprobarEstadoRegistro(indiceLog int) {
	indices := make([]int, cfg.numReplicas)
	terms := make([]int, cfg.numReplicas)
	for i := range cfg.nodosRaft {
		indices[i], terms[i] = cfg.obtenerEstadoRegistro(i)
	}
}

func (cfg *configDespliegue) obtenerEstadoRegistro(indiceNodo int) (int, int) {
	var reply raft.EstadoRegistro
	err := cfg.nodosRaft[indiceNodo].CallTimeout("NodoRaft.ObtenerEstadoRegistro",
		raft.Vacio{}, &reply, 10*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC ObtenerEstadoRegistro")
	return reply.Indice, reply.Mandato
}
