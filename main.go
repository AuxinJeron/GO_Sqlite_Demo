package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type MetaCommandResult int32
type PrepareStatementResult int32
type StatementType int32
type Statement struct {
	statementType StatementType
}

const (
	META_COMMAND_SUCCESS MetaCommandResult = iota
	META_COMMAND_UNRECOGNIZED
)
const (
	PREPARE_STATEMENT_SUCCESS PrepareStatementResult = iota
	PREPARE_STATEMENT_UNRECOGNIZED
)

const (
	STATEMENT_INSERT StatementType = iota
	STATEMENT_SELECT
)

func main() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Simple SQLite")
	fmt.Println("---------------------")

	for {
		print_prompt()
		command, _ := reader.ReadString('\n')
		// convert CRLF to LF
		command = strings.Replace(command, "\n", "", -1)

		if command[0] == '.' {
			switch do_meta_command(command) {
			case (META_COMMAND_SUCCESS):
				continue
			case (META_COMMAND_UNRECOGNIZED):
				fmt.Printf("Unrecognized command '%s'.\n", command)
				continue
			}
		}

		statement := Statement{}
		switch prepare_statement(command, &statement) {
		case (PREPARE_STATEMENT_SUCCESS):
			break
		case (PREPARE_STATEMENT_UNRECOGNIZED):
			fmt.Printf("Unrecognized command '%s'.\n", command)
			continue
		}
		execute_statement(&statement)
		fmt.Println("Executed.")
	}
}

func print_prompt() {
	fmt.Print("db > ")
}

func do_meta_command(command string) MetaCommandResult {
	if strings.Compare(".exit", command) == 0 {
		os.Exit(0)
	} else {
		return META_COMMAND_UNRECOGNIZED
	}
	return META_COMMAND_UNRECOGNIZED
}

func prepare_statement(command string, statement *Statement) PrepareStatementResult {
	if strings.Compare(command, "insert") == 0 {
		statement.statementType = STATEMENT_INSERT
		return PREPARE_STATEMENT_SUCCESS
	}
	if strings.Compare(command, "select") == 0 {
		statement.statementType = STATEMENT_SELECT
		return PREPARE_STATEMENT_SUCCESS
	}
	return PREPARE_STATEMENT_UNRECOGNIZED
}

func execute_statement(statement *Statement) {
	switch statement.statementType {
	case (STATEMENT_INSERT):
		fmt.Println("This is where we would do an insert.")
		break
	case (STATEMENT_SELECT):
		fmt.Println("This is where we would do a select.")
		break
	}
}
