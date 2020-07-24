package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strings"
)

const ID_SIZE = 4
const USER_NAME_SIZE = 32
const EMAIL_SIZE = 255
const ID_OFFSET = 0
const USER_NAME_OFFSET = ID_OFFSET + ID_SIZE
const EMAIL_OFFSET = USER_NAME_OFFSET + USER_NAME_SIZE
const ROW_SIZE = ID_SIZE + USER_NAME_SIZE + EMAIL_SIZE
const PAGE_SIZE = 4096
const TABLE_MAX_PAGES = 100
const ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE
const TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES

type MetaCommandResult int32
type PrepareStatementResult int32
type StatementType int32
type ExecuteResult int32

type Statement struct {
	statementType StatementType
	rowToInsert   Row
}

type Row struct {
	id       uint32
	username string
	email    string
}

type Table struct {
	numrows uint32
	pages   [][]byte
}

const (
	META_COMMAND_SUCCESS MetaCommandResult = iota
	META_COMMAND_UNRECOGNIZED
)
const (
	PREPARE_STATEMENT_SUCCESS PrepareStatementResult = iota
	PREPARE_STATEMENT_UNRECOGNIZED
	PREPARE_SYNTAX_ERROR
)

const (
	STATEMENT_INSERT StatementType = iota
	STATEMENT_SELECT
)

const (
	EXECUTE_SUCCESS ExecuteResult = iota
	EXECUTE_TABLE_FULL
	EXECUTE_FAILURE
)

func main() {
	table := new_table()

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
		case (PREPARE_SYNTAX_ERROR):
			fmt.Printf("Syntax error. Could not parse statement.\n")
			continue
		case (PREPARE_STATEMENT_UNRECOGNIZED):
			fmt.Printf("Unrecognized command '%s'.\n", command)
			continue
		}
		execute_statement(&statement, &table)
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

func prepare_statement(cmdStr string, statement *Statement) PrepareStatementResult {
	cmdArgs := strings.Split(cmdStr, " ")

	if strings.Compare(cmdArgs[0], "insert") == 0 {
		statement.statementType = STATEMENT_INSERT
		args, _ := fmt.Sscanf(cmdStr, "insert %d %s %s", &statement.rowToInsert.id, &statement.rowToInsert.username, &statement.rowToInsert.email)
		if args < 3 {
			return PREPARE_SYNTAX_ERROR
		}
		return PREPARE_STATEMENT_SUCCESS
	}
	if strings.Compare(cmdArgs[0], "select") == 0 {
		statement.statementType = STATEMENT_SELECT
		return PREPARE_STATEMENT_SUCCESS
	}
	return PREPARE_STATEMENT_UNRECOGNIZED
}

func execute_statement(statement *Statement, table *Table) ExecuteResult {
	switch statement.statementType {
	case (STATEMENT_INSERT):
		return execute_insert(statement, table)
	case (STATEMENT_SELECT):
		return execute_select(statement, table)
	}
	return EXECUTE_FAILURE
}

func serialize_row(src Row) []byte {
	idBytes := make([]byte, ID_SIZE)
	binary.LittleEndian.PutUint32(idBytes, uint32(src.id))
	usernameBytes := make([]byte, USER_NAME_SIZE)
	copy(usernameBytes, src.username)
	emailBytes := make([]byte, EMAIL_SIZE)
	copy(emailBytes, src.email)
	return bytes.Join([][]byte{idBytes, usernameBytes, emailBytes}, []byte{})
}

func deserialize_row(src []byte) Row {
	var dst Row
	binary.Read(bytes.NewBuffer(src[ID_OFFSET:USER_NAME_OFFSET]), binary.LittleEndian, &dst.id)
	dst.username = string(bytes.Trim(src[USER_NAME_OFFSET:EMAIL_OFFSET], "\x00"))
	dst.email = string(bytes.Trim(src[EMAIL_OFFSET:], "\x00"))
	return dst
}

func row_slot(table *Table, rownum uint32) []byte {
	pagenum := rownum / ROWS_PER_PAGE
	page := table.pages[pagenum]
	if page == nil {
		table.pages[pagenum] = make([]byte, PAGE_SIZE)
		page = table.pages[pagenum]
	}
	rowOffset := rownum % ROWS_PER_PAGE
	byteOffset := rowOffset * ROW_SIZE
	return page[byteOffset : byteOffset+ROW_SIZE+1]
}

func execute_insert(statement *Statement, table *Table) ExecuteResult {
	if table.numrows >= TABLE_MAX_ROWS {
		return EXECUTE_TABLE_FULL
	}
	copy(row_slot(table, table.numrows), serialize_row(statement.rowToInsert))
	table.numrows += 1
	return EXECUTE_SUCCESS
}

func execute_select(statement *Statement, table *Table) ExecuteResult {
	var i uint32
	for i = 0; i < table.numrows; i++ {
		row := deserialize_row(row_slot(table, i))
		fmt.Println(row)
	}
	return EXECUTE_SUCCESS
}

func new_table() Table {
	var table Table
	table.numrows = 0
	table.pages = make([][]byte, TABLE_MAX_PAGES)
	return table
}
