package main

import (
	//"errors"
	//"fmt"
	//"log"
	"net"
	"net/rpc"
	"os"
	"raft/internal/raft"
	"raft/internal/comun/rpctimeout"
	"raft/internal/comun/check"
	"strconv"
	//"time"
)


func main() {
	// obtener entero de indice de este nodo
	datos:= make(map[string]string)

	me, err := strconv.Atoi(os.Args[1])
	check.CheckError(err, "Main, mal numero entero de indice de nodo:")

	var nodos []rpctimeout.HostPort
	// Resto de argumento son los end points como strings
	// De todas la replicas-> pasarlos a HostPort
	for _, endPoint := range os.Args[2:] {
		 nodos = append(nodos, rpctimeout.HostPort(endPoint))
	}

	canalAplicarOperacion := make(chan raft.AplicaOperacion, 1000)
    canalRes := make(chan raft.AplicaOperacion, 1000)

	// Parte Servidor
	nr := raft.NuevoNodo(nodos, me, canalAplicarOperacion,canalRes)
	rpc.Register(nr)
	
	go aplicarOperacion(nr, datos, canalAplicarOperacion, canalRes)

	//fmt.Println("Replica escucha en :", me, " de ", os.Args[2:])

	l, err := net.Listen("tcp", os.Args[2:][me])
	check.CheckError(err, "Main listen error:")

	rpc.Accept(l)
}

func aplicarOperacion(nr *raft.NodoRaft, almacen map[string]string, canalOper chan raft.AplicaOperacion, canalRes chan raft.AplicaOperacion) {
    for {
        operacion := <-canalOper
        nr.Logger.Println("Main ha recibido la operación: ", operacion)
        if operacion.Operacion.Operacion == "leer" {
            operacion.Operacion.Valor = almacen[operacion.Operacion.Clave]
        } else if operacion.Operacion.Operacion == "escribir" {
            almacen[operacion.Operacion.Clave] = operacion.Operacion.Valor
            operacion.Operacion.Valor = "ok"
        }
        nr.Logger.Println("Aplicando operacion: ", operacion)
        if nr.Yo == nr.IdLider {
            canalRes <- operacion
            nr.Logger.Println("Respuesta enviada: ", operacion)
        }
        nr.Logger.Println("Esperando siguiente operacion")
    }
}

