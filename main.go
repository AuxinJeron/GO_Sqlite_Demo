package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"unsafe"
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

/*
 * Common Node Header Layout
 */
const NODE_TYPE_SIZE = 1
const NODE_TYPE_OFFSET = 0
const IS_ROOT_SIZE = 1
const IS_ROOT_OFFSET = NODE_TYPE_SIZE
const PARENT_POINTER_SIZE = 4
const PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE
const COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE

/*
 * Leaf Node Header Layout
 * Leaf Node contains list of cells, each cell is a key/value pair
 */
const LEAF_NODE_NUM_CELLS_SIZE = 4
const LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE
const LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE

/*
 * Leaf Node Body Layout
 */
const LEAF_NODE_KEY_SIZE = 4
const LEAF_NODE_KEY_OFFSET = 0
const LEAF_NODE_VALUE_SIZE = ROW_SIZE
const LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE
const LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE
const LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE
const LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE

type MetaCommandResult int32
type PrepareStatementResult int32
type StatementType int32
type ExecuteResult int32
type NodeType uint8

type Statement struct {
	statementType StatementType
	rowToInsert   *Row
}

type Row struct {
	id       uint32
	username string
	email    string
}

type Pager struct {
	fileDescriptor *os.File
	fileLength     uint32
	numPages       uint32
	pages          [][]byte
}

type Table struct {
	rootPageNum uint32
	pager       *Pager
}

type Cursor struct {
	table      *Table
	pageNum    uint32
	cellNum    uint32
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
	EXECUTE_DUPLICATE_KEY
)

const (
	NODE_INTERNAL NodeType = iota
	NODE_LEAF
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
		statement.rowToInsert = &Row{}
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
		case (EXECUTE_DUPLICATE_KEY):
			fmt.Printf("Error: Duplicate key.\n")
			break
		case (EXECUTE_TABLE_FULL):
			fmt.Println("Error: Table full.")
			break
		}
	}
}

func print_prompt() {
	fmt.Print("db > ")
}

func print_constants() {
	fmt.Printf("ROW_SIZE: %d\n", ROW_SIZE)
	fmt.Printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE)
	fmt.Printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE)
	fmt.Printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE)
	fmt.Printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS)
	fmt.Printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS)
}

func print_leaf_node(node []byte) {
	numCells := *leaf_node_num_cells(node)
	fmt.Printf("leaf (size %d)\n", numCells)
	for i := uint32(0); i < numCells; i++ {
		key := *leaf_node_cell_key(node, i)
		fmt.Printf("  - %d : %d\n", i, key)
	}
}

func do_meta_command(command string, table *Table) MetaCommandResult {
	if strings.Compare(".exit", command) == 0 {
		db_close(table)
		os.Exit(0)
	} else if strings.Compare(".btree", command) == 0 {
		fmt.Printf("Tree:\n")
		print_leaf_node(get_page(table.pager, 0))
		return META_COMMAND_SUCCESS
	} else if strings.Compare(".constants", command) == 0 {
		fmt.Printf("Constants:\n")
		print_constants()
		return META_COMMAND_SUCCESS
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

func serialize_row(src *Row) []byte {
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
	pagenum := cursor.pageNum
	page := get_page(cursor.table.pager, pagenum)
	return leaf_node_cell_value(page, cursor.cellNum)
}

func execute_insert(statement *Statement, table *Table) ExecuteResult {
	node := get_page(table.pager, table.rootPageNum)
	numCells := *leaf_node_num_cells(node)
	if numCells >= LEAF_NODE_MAX_CELLS {
		return EXECUTE_TABLE_FULL
	}
	keyToInsert := statement.rowToInsert.id
	cursor := table_find(table, keyToInsert)

	if cursor.cellNum < numCells {
		// check whether the key exists
		keyAtIndex := *leaf_node_cell_key(node, cursor.cellNum)
		if keyAtIndex == keyToInsert {
			return EXECUTE_DUPLICATE_KEY
		}
	}
	leaf_node_insert(cursor, statement.rowToInsert.id, statement.rowToInsert)
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
	table.rootPageNum = 0
	if pager.numPages == 0 {
		// New database file. Initial page 0 as leaf node
		rootNode := get_page(pager, 0)
		initialize_leaf_node(rootNode)
	}
	return table
}

func db_close(table *Table) {
	pager := table.pager

	for i := uint32(0); i < pager.numPages; i++ {
		if pager.pages[i] == nil {
			continue
		}
		pager_flush(pager, i)
		pager.pages[i] = nil
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
	if offset%PAGE_SIZE != 0 {
		fmt.Printf("DB file is not a whole number of pages.\n")
		os.Exit(1)
	}
	// Init the pager based on the persistent file
	pager := new(Pager)
	pager.fileDescriptor = fd
	pager.fileLength = uint32(offset)
	pager.numPages = uint32(offset / PAGE_SIZE)

	pager.pages = make([][]byte, TABLE_MAX_PAGES)

	return pager
}

func pager_close(pager *Pager) {
	if err := pager.fileDescriptor.Close(); err != nil {
		fmt.Printf("Error closing db file. %v.\n", err)
		os.Exit(1)
	}
}

func get_page(pager *Pager, pagenum uint32) []byte {
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
			pager.fileDescriptor.Seek(int64(PAGE_SIZE*pagenum), 0)
			_, err := pager.fileDescriptor.Read(pager.pages[pagenum])
			if err != nil {
				fmt.Printf("Error reading file. %v\n", err)
				os.Exit(1)
			}
		}

		if pagenum >= pager.numPages {
			pager.numPages = pagenum + 1
		}
	}

	return pager.pages[pagenum]
}

func pager_flush(pager *Pager, pagenum uint32) {
	if pager.pages[pagenum] == nil {
		fmt.Printf("Tried to flush null page.\n")
		os.Exit(1)
	}

	_, err := pager.fileDescriptor.Seek(int64(pagenum*PAGE_SIZE), 0)
	if err != nil {
		fmt.Printf("Error seeking. %v\n", err)
		os.Exit(1)
	}

	_, err = pager.fileDescriptor.Write(pager.pages[pagenum][0:PAGE_SIZE])
	if err != nil {
		fmt.Printf("Error writing. %v\n", err)
	}
}

func table_start(table *Table) *Cursor {
	cursor := new(Cursor)
	cursor.table = table
	cursor.pageNum = table.rootPageNum
	cursor.cellNum = 0

	rootNode := get_page(table.pager, table.rootPageNum)
	numCells := leaf_node_num_cells(rootNode)
	cursor.endOfTable = (*numCells == 0)
	return cursor
}

/*
Return the position of the given key.
If the key is not present, return the position where it should be inserted
*/
func table_find(table *Table, key uint32) *Cursor {
	rootPageNum := table.rootPageNum
	rootNode := get_page(table.pager, rootPageNum)
	if get_node_type(rootNode) == NODE_LEAF {
		return leaf_node_find(table, rootPageNum, key)
	} else {
		fmt.Printf("Need to implement searching an internal node\n")
		os.Exit(1)
	}
	return nil
}

func cursor_advance(cursor *Cursor) {
	pageNum := cursor.pageNum
	node := get_page(cursor.table.pager, pageNum)
	cursor.cellNum += 1
	// Why it is end of table if the cell num is greater than num cells
	cursor.endOfTable = (cursor.cellNum >= *leaf_node_num_cells(node))
}

func get_node_type(node []byte) NodeType {
	value := node[NODE_TYPE_OFFSET]
	return NodeType(value)
}

func set_node_type(node []byte, nodeType NodeType) {
	node[NODE_TYPE_OFFSET] = byte(nodeType)
}

func leaf_node_num_cells(node []byte) *uint32 {
	return (*uint32)(unsafe.Pointer(&node[LEAF_NODE_NUM_CELLS_OFFSET]))
}

func leaf_node_cell(node []byte, cellNum uint32) []byte {
	offset := LEAF_NODE_HEADER_SIZE + cellNum*LEAF_NODE_CELL_SIZE
	return node[offset : offset+LEAF_NODE_CELL_SIZE]
}

func leaf_node_cell_key(node []byte, cellNum uint32) *uint32 {
	return (*uint32)(unsafe.Pointer(&(leaf_node_cell(node, cellNum)[LEAF_NODE_KEY_OFFSET])))
}

func leaf_node_cell_value(node []byte, cellNum uint32) []byte {
	return leaf_node_cell(node, cellNum)[LEAF_NODE_VALUE_OFFSET : LEAF_NODE_VALUE_OFFSET+LEAF_NODE_VALUE_SIZE]
}

func initialize_leaf_node(node []byte) {
	set_node_type(node, NODE_LEAF)
	*leaf_node_num_cells(node) = 0
}

func leaf_node_insert(cursor *Cursor, key uint32, row *Row) {
	node := get_page(cursor.table.pager, cursor.pageNum)
	numCells := *leaf_node_num_cells(node)

	if numCells >= LEAF_NODE_MAX_CELLS {
		fmt.Printf("Need to implement spliting a leaf node. \n")
		os.Exit(1)
	}

	if cursor.cellNum < numCells {
		// Make room for new cell
		for i := numCells; i > cursor.cellNum; i-- {
			copy(leaf_node_cell(node, i), leaf_node_cell(node, i-1))
		}
	}
	// Insert the row
	*leaf_node_num_cells(node) += 1
	*leaf_node_cell_key(node, cursor.cellNum) = key
	copy(leaf_node_cell_value(node, cursor.cellNum), serialize_row(row))
}

func leaf_node_find(table *Table, pageNum uint32, key uint32) *Cursor {
	node := get_page(table.pager, pageNum)
	numCells := *leaf_node_num_cells(node)

	cursor := &Cursor{}
	cursor.table = table
	cursor.pageNum = pageNum

	// binary search
	minIndex := uint32(0)
	onePastMaxIndex := numCells
	for onePastMaxIndex != minIndex {
		index := (minIndex + onePastMaxIndex) / 2
		keyAtIndex := *leaf_node_cell_key(node, index)
		if key == keyAtIndex {
			cursor.cellNum = index
			return cursor
		}
		if key < keyAtIndex {
			onePastMaxIndex = index
		} else {
			minIndex = index + 1
		}
	}

	cursor.cellNum = minIndex
	return cursor
}
