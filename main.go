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

type Pager struct {
	fileDescriptor *os.File
	fileLength     int64
	pages          [][]byte
}

type Table struct {
	numrows int64
	pager   *Pager
}

type Cursor struct {
	table      *Table
	rowNum     int64
	endOfTable bool // indicates a position one past the last element
}

const (
	META_COMMAND_SUCCESS MetaCommandResult = iota
	META_COMMAND_UNRECOGNIZED
)
const (
	PREPARE_STATEMENT_SUCCESS PrepareStatementResult = iota
	PREPARE_STATEMENT_UNRECOGNIZED
	PREPARE_STRING_TOO_LONG
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

	if len(os.Args) < 2 {
		fmt.Printf("Must supply a database filename.\n")
		os.Exit(1)
	}

	filename := os.Args[1]
	table := db_open(filename)

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Simple SQLite")
	fmt.Println("---------------------")

	for {
		print_prompt()
		command, _ := reader.ReadString('\n')
		// convert CRLF to LF
		command = strings.Replace(command, "\n", "", -1)

		if command[0] == '.' {
			switch do_meta_command(command, table) {
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
		case (PREPARE_STRING_TOO_LONG):
			fmt.Printf("String is too long.\n")
			continue
		case (PREPARE_STATEMENT_UNRECOGNIZED):
			fmt.Printf("Unrecognized command '%s'.\n", command)
			continue
		}

		switch execute_statement(&statement, table) {
		case (EXECUTE_SUCCESS):
			fmt.Println("Executed.")
			break
		case (EXECUTE_TABLE_FULL):
			fmt.Println("Error: Table full.")
		}
	}
}

func print_prompt() {
	fmt.Print("db > ")
}

func do_meta_command(command string, table *Table) MetaCommandResult {
	if strings.Compare(".exit", command) == 0 {
		db_close(table)
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
		if len(statement.rowToInsert.username) > USER_NAME_SIZE {
			return PREPARE_STRING_TOO_LONG
		}
		if len(statement.rowToInsert.email) > EMAIL_SIZE {
			return PREPARE_STRING_TOO_LONG
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

func cursor_value(cursor *Cursor) []byte {
	pagenum := cursor.rowNum / ROWS_PER_PAGE
	page := get_page(cursor.table.pager, pagenum)
	rowOffset := cursor.rowNum % ROWS_PER_PAGE
	byteOffset := rowOffset * ROW_SIZE
	return page[byteOffset : byteOffset+ROW_SIZE+1]
}

func execute_insert(statement *Statement, table *Table) ExecuteResult {
	if table.numrows >= TABLE_MAX_ROWS {
		return EXECUTE_TABLE_FULL
	}
	cursor := table_end(table)
	copy(cursor_value(cursor), serialize_row(statement.rowToInsert))
	table.numrows += 1
	return EXECUTE_SUCCESS
}

func execute_select(statement *Statement, table *Table) ExecuteResult {
	cursor := table_start(table)

	for !cursor.endOfTable {
		row := deserialize_row(cursor_value(cursor))
		fmt.Println(row)
		cursor_advance(cursor)
	}
	return EXECUTE_SUCCESS
}

func db_open(filename string) *Table {
	pager := pager_open(filename)
	table := new(Table)
	table.pager = pager
	table.numrows = pager.fileLength / ROW_SIZE
	return table
}

func db_close(table *Table) {
	pager := table.pager
	numFullPages := table.numrows / ROWS_PER_PAGE

	for i := int64(0); i < numFullPages; i++ {
		if pager.pages[i] == nil {
			continue
		}
		pager_flush(pager, i, PAGE_SIZE)
		pager.pages[i] = nil
	}

	numAdditionalRows := table.numrows % ROWS_PER_PAGE
	if numAdditionalRows > 0 {
		if pager.pages[numFullPages] != nil {
			pager_flush(pager, numFullPages, numAdditionalRows*ROW_SIZE)
			pager.pages[numFullPages] = nil
		}
	}

	pager_close(pager)
}

func pager_open(filename string) *Pager {
	// Read the persistent file
	fd, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		fmt.Printf("Unable to open file.\n")
		os.Exit(1)
	}
	offset, err := fd.Seek(0, 2)
	// Init the pager based on the persistent file
	pager := new(Pager)
	pager.fileDescriptor = fd
	pager.fileLength = offset
	pager.pages = make([][]byte, TABLE_MAX_PAGES)

	return pager
}

func pager_close(pager *Pager) {
	if err := pager.fileDescriptor.Close(); err != nil {
		fmt.Printf("Error closing db file. %v.\n", err)
		os.Exit(1)
	}
}

func get_page(pager *Pager, pagenum int64) []byte {
	if pagenum > TABLE_MAX_PAGES {
		fmt.Printf("Tried to fetch page number out of bounds. %d > %d\n", pagenum, TABLE_MAX_PAGES)
	}

	if pager.pages[pagenum] == nil {
		// Cache miss. Allocate memory and load from file
		pager.pages[pagenum] = make([]byte, PAGE_SIZE)
		totalpages := pager.fileLength / PAGE_SIZE
		if pager.fileLength%PAGE_SIZE != 0 {
			totalpages += 1
		}

		// Load the bytes to page if the page num exists in the persistent file
		if pagenum < totalpages {
			pager.fileDescriptor.Seek(PAGE_SIZE*pagenum, 0)
			_, err := pager.fileDescriptor.Read(pager.pages[pagenum])
			if err != nil {
				fmt.Printf("Error reading file. %v\n", err)
				os.Exit(1)
			}
		}
	}

	return pager.pages[pagenum]
}

func pager_flush(pager *Pager, pagenum int64, size int64) {
	if pager.pages[pagenum] == nil {
		fmt.Printf("Tried to flush null page.\n")
		os.Exit(1)
	}

	_, err := pager.fileDescriptor.Seek(pagenum*PAGE_SIZE, 0)
	if err != nil {
		fmt.Printf("Error seeking. %v\n", err)
		os.Exit(-1)
	}

	_, err = pager.fileDescriptor.Write(pager.pages[pagenum][0:size])
	if err != nil {
		fmt.Printf("Error writing. %v\n", err)
	}
}

func table_start(table *Table) *Cursor {
	cursor := new(Cursor)
	cursor.table = table
	cursor.rowNum = 0
	cursor.endOfTable = (table.numrows == 0)
	return cursor
}

func table_end(table *Table) *Cursor {
	cursor := new(Cursor)
	cursor.table = table
	cursor.rowNum = table.numrows
	cursor.endOfTable = true
	return cursor
}

func cursor_advance(cursor *Cursor) {
	cursor.rowNum += 1
	cursor.endOfTable = (cursor.rowNum >= cursor.table.numrows)
}
