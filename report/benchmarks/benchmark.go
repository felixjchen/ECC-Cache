package main

import (
	"fmt"
	"math/rand"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// https://tikv.org/blog/tikv-3.0ga/

const ecc_prefix = "distributed_cache ecc client"
const raft_prefix = "distributed_cache raft client"

const records = 10000

func get_stop_raft_node_command() []string {
	return strings.Split("docker kill d94_raft3_1", " ")
}
func get_start_raft_node_command() []string {
	return strings.Split("docker run -d -p 4002:4002 -e DOCKER_HOSTNAME=host.docker.internal --name d94_raft3_replacement_1 felixchen1998/distributed-cache-server:latest raft server startOne 2", " ")
}

func get_stop_ecc_node_command() []string {
	return strings.Split("docker kill d94_ecc3_1", " ")
}
func get_start_ecc_node_command() []string {
	return strings.Split("docker run -d -p 3002:3002 -e DOCKER_HOSTNAME=host.docker.internal --name d94_ecc3_replacement_1 felixchen1998/distributed-cache-server:latest ecc server startOne 0.0.0.0:3002 recover", " ")
}

func get_disconnect_ecc_node_command() []string {
	return strings.Split("docker network disconnect d94_default d94_ecc3_1", " ")
}
func get_connect_ecc_node_command() []string {
	return strings.Split("docker network connect d94_default d94_ecc3_1", " ")
}

func get_set_command(key_number int) string {
	set_command := "key" + strconv.Itoa(key_number) + " value" + strconv.Itoa(rand.Intn(records))
	return set_command
}

func insert(a [][]string, index int, value []string) [][]string {
	a = append(a[:index+1], a[index:]...) // Step 1+2
	a[index] = value                      // Step 3
	return a
}

func get_get_command(key_number int) string {
	return "key" + strconv.Itoa(key_number)
}

func get_commands(prefix string, count int, command_fn func(int) string) [][]string {
	var res [][]string
	for i := 0; i < count; i++ {
		command := strings.Split(prefix+command_fn(i), " ")
		res = append(res, command)
	}
	return res
}

func shuffle_workload(workload [][]string) {
	rand.Shuffle(len(workload), func(i int, j int) {
		workload[i], workload[j] = workload[j], workload[i]
	})
}

func get_workload_A(prefix string) [][]string {
	// 50 Read / 50 Read
	reads := 0.5 * records
	writes := 0.5 * records

	write_workload := get_commands(prefix+" set ", int(writes), get_set_command)
	read_workload := get_commands(prefix+" get ", int(reads), get_get_command)
	workload := append(write_workload, read_workload...)
	shuffle_workload(workload)

	return workload
}

func get_workload_B(prefix string) [][]string {
	// 95 Read / 5 Write
	reads := 0.95 * records
	writes := 0.05 * records

	write_workload := get_commands(prefix+" set ", int(writes), get_set_command)
	read_workload := get_commands(prefix+" get ", int(reads), get_get_command)
	workload := append(write_workload, read_workload...)
	shuffle_workload(workload)

	return workload
}

func get_workload_C(prefix string) [][]string {
	// 100 Write
	writes :=  records

	workload := get_commands(prefix+" set ", int(writes), get_set_command)
	shuffle_workload(workload)

	return workload
}

func run_ecc(workload [][]string) {
	cmd := exec.Command("docker", strings.Fields("compose -f docker-compose-ecc.yml up -d")...)
	stdout, _ := cmd.Output()
	fmt.Println(string(stdout))
	fmt.Println("Started ecc cache")

	start := time.Now()
	for i, command := range workload {
		cmd := exec.Command(command[0], command[1:]...)
		stdout, _ := cmd.Output()
		fmt.Println(i, string(stdout))
	}
	elapsed := time.Since(start)
	fmt.Printf("Benchmark took %s", elapsed)
	fmt.Printf("Average transaction time %s seconds", elapsed.Seconds()/records)

	cmd = exec.Command("docker", strings.Fields("compose -f docker-compose-ecc.yml down")...)
	stdout, _ = cmd.Output()
	fmt.Println(string(stdout))
	fmt.Println("Stopped ecc cache")
}

func run_raft(workload [][]string) {
	cmd := exec.Command("docker", strings.Fields("compose -f docker-compose-raft.yml up -d")...)
	stdout, _ := cmd.Output()
	fmt.Println(string(stdout))
	fmt.Println("Started raft cache")

	start := time.Now()
	for i, command := range workload {
		cmd := exec.Command(command[0], command[1:]...)
		stdout, _ := cmd.Output()
		fmt.Println(i, string(stdout))
	}
	elapsed := time.Since(start)
	fmt.Printf("Benchmark took %s", elapsed)
	fmt.Printf("Average transaction time %s seconds", elapsed.Seconds()/records)

	cmd = exec.Command("docker", strings.Fields("compose -f docker-compose-raft.yml down")...)
	stdout, _ = cmd.Output()
	fmt.Println(string(stdout))
	fmt.Println("Stopped raft cache")
}

func run_raft_test() {
	workload := get_workload_C(raft_prefix)
	fmt.Println("Generated workload")
	// insert(workload, 3333, get_stop_raft_node_command())
	// insert(workload, 6666, get_start_raft_node_command())

	run_raft(workload)
	// run_raft(workload)
	// run_raft(workload)
}


func run_ecc_test() {
	workload := get_workload_C(ecc_prefix)
	fmt.Println("Generated workload")
	// insert(workload, 3333, get_disconnect_ecc_node_command())
	// insert(workload, 6666, get_connect_ecc_node_command())

	run_ecc(workload)
}

func main() {
	run_raft_test()
}
