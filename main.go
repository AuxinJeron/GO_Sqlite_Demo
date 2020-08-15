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
const LEAF_NODE_NEXT_LEAF_SIZE = 4
const LEAF_NODE_NEXT_LEAF_OFFSET = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE
const LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE

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
const LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2
const LEAF_NODE_LEFT_SPLIT_COUNT = LEAF_NODE_MAX_CELLS + 1 - LEAF_NODE_RIGHT_SPLIT_COUNT

/*
 * Internal Node Header Layout
 */
const INTERNAL_NODE_NUM_KEYS_SIZE = 4
const INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE
const INTERNAL_NODE_RIGHT_CHILD_SIZE = 4 // Store the rightmore child page number
const INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE
const INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE

/*
 * Internal Node Body Layout
 */
const INTENRAL_NODE_KEY_SIZE = 4   // Store key id
const INTENRAL_NODE_CHILD_SIZE = 4 // Store child page number
const INTERNAL_NODE_CELL_SIZE = INTENRAL_NODE_KEY_SIZE + INTENRAL_NODE_CHILD_SIZE
const INTERNAL_NODE_MAX_CELLS = 3

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

func indent(level uint32) {
	for i := uint32(0); i < level; i++ {
		fmt.Printf("  ")
	}
}

func print_tree(pager *Pager, pageNum uint32, indentationLevel uint32) {
	node := get_page(pager, pageNum)

	switch get_node_type(node) {
	case NODE_LEAF:
		numKeys := *leaf_node_num_cells(node)
		indent(indentationLevel)
		fmt.Printf("- leaf (size %d)\n", numKeys)
		for i := uint32(0); i < numKeys; i++ {
			indent(indentationLevel + 1)

			fmt.Printf("- %d\n", *leaf_node_cell_key(node, i))
		}
		break
	case NODE_INTERNAL:
		numKeys := *internal_node_num_keys(node)
		indent(indentationLevel)
		fmt.Printf("- internal (size %d)\n", numKeys)
		for i := uint32(0); i < numKeys; i++ {
			child := *internal_node_child(node, i)
			print_tree(pager, child, indentationLevel+1)
			indent(indentationLevel + 1)
			fmt.Printf("- key %d\n", *internal_node_cell_key(node, i))
		}
		child := *internal_node_right_child(node)
		print_tree(pager, child, indentationLevel+1)
		break
	}
}

func do_meta_command(command string, table *Table) MetaCommandResult {
	if strings.Compare(".exit", command) == 0 {
		db_close(table)
		os.Exit(0)
	} else if strings.Compare(".btree", command) == 0 {
		fmt.Printf("Tree:\n")
		print_tree(table.pager, 0, 0)
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
		set_node_root(rootNode, true)
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

func get_unused_page_num(pager *Pager) uint32 {
	return pager.numPages
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
	cursor := table_find(table, 0)

	node := get_page(table.pager, cursor.pageNum)
	numCells := *leaf_node_num_cells(node)
	cursor.endOfTable = (numCells == 0)

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
		return internal_node_find(table, rootPageNum, key)
	}
}

func cursor_advance(cursor *Cursor) {
	pageNum := cursor.pageNum
	node := get_page(cursor.table.pager, pageNum)
	cursor.cellNum += 1

	/* Advance to next leaf node */
	if cursor.cellNum >= *leaf_node_num_cells(node) {
		nextPageNum := *leaf_node_next_leaf(node)
		if nextPageNum == 0 {
			// This was rightmost leaf
			cursor.endOfTable = true
		} else {
			cursor.pageNum = nextPageNum
			cursor.cellNum = 0
		}
	}
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
	set_node_root(node, false)
	*leaf_node_num_cells(node) = 0
	*leaf_node_next_leaf(node) = 0 // 0 represents no sibling
}

func initialize_internal_node(node []byte) {
	set_node_type(node, NODE_INTERNAL)
	set_node_root(node, false)
	*internal_node_num_keys(node) = 0
}

func leaf_node_insert(cursor *Cursor, key uint32, row *Row) {
	node := get_page(cursor.table.pager, cursor.pageNum)
	numCells := *leaf_node_num_cells(node)

	if numCells >= LEAF_NODE_MAX_CELLS {
		leaf_node_split_and_insert(cursor, row)
		return
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

func internal_node_find_child(node []byte, key uint32) uint32 {
	/*
	 * Return the index of the child which should contain the given key
	 */
	numKeys := *internal_node_num_keys(node)
	/* Binary search to find the index of child to search */
	minIndex := uint32(0)
	maxIndex := numKeys

	for minIndex != maxIndex {
		index := (minIndex + maxIndex) / 2
		keyToRight := *internal_node_cell_key(node, index)
		if keyToRight >= key {
			maxIndex = index
		} else {
			minIndex = index + 1
		}
	}
	return minIndex
}

func internal_node_find(table *Table, pageNum uint32, key uint32) *Cursor {
	node := get_page(table.pager, pageNum)

	childIndex := internal_node_find_child(node, key)
	childNum := *internal_node_child(node, childIndex)
	child := get_page(table.pager, childNum)
	switch get_node_type(child) {
	case NODE_LEAF:
		return leaf_node_find(table, childNum, key)
	case NODE_INTERNAL:
		return internal_node_find(table, childNum, key)
	}
	return nil
}

func leaf_node_split_and_insert(cursor *Cursor, value *Row) {
	/*
		Create a new node and move half the cells over.
		Insert the new value in one of the two nodes.
		Update parent or create a new parent
	*/
	oldNode := get_page(cursor.table.pager, cursor.pageNum)
	oldMax := get_node_max_key(oldNode)
	newPageNum := get_unused_page_num(cursor.table.pager)
	newNode := get_page(cursor.table.pager, newPageNum)
	initialize_leaf_node(newNode)
	*node_parent(newNode) = *node_parent(oldNode)
	*leaf_node_next_leaf(newNode) = *leaf_node_next_leaf(oldNode)
	*leaf_node_next_leaf(oldNode) = newPageNum

	/*
		All existing keys plus new key should be divided
		evenly between old (left) and new (right) nodes.
		Starting from the right, move each key to correct position.
	*/
	for i := int(LEAF_NODE_MAX_CELLS); i >= 0; i-- {
		var destNode []byte
		if i >= LEAF_NODE_LEFT_SPLIT_COUNT {
			destNode = newNode
		} else {
			destNode = oldNode
		}
		destCellIndex := uint32(i % LEAF_NODE_LEFT_SPLIT_COUNT)
		destCell := leaf_node_cell(destNode, destCellIndex)

		if i == int(cursor.cellNum) {
			*leaf_node_cell_key(destNode, destCellIndex) = value.id
			copy(leaf_node_cell_value(destNode, destCellIndex), serialize_row(value))
		} else if i > int(cursor.cellNum) {
			copy(destCell, leaf_node_cell(oldNode, uint32(i-1)))
		} else {
			copy(destCell, leaf_node_cell(oldNode, uint32(i)))
		}
	}

	/* Update cell count on both leaf nodes */
	*leaf_node_num_cells(oldNode) = LEAF_NODE_LEFT_SPLIT_COUNT
	*leaf_node_num_cells(newNode) = LEAF_NODE_RIGHT_SPLIT_COUNT

	if is_node_root(oldNode) {
		create_new_root(cursor.table, newPageNum)
	} else {
		parentPageNum := *node_parent(oldNode)
		newMax := get_node_max_key(oldNode)
		parent := get_page(cursor.table.pager, parentPageNum)
		update_internal_node_key(parent, oldMax, newMax)
		internal_node_insert(cursor.table, parentPageNum, newPageNum)
	}
}

func leaf_node_next_leaf(node []byte) *uint32 {
	return (*uint32)(unsafe.Pointer(&node[LEAF_NODE_NEXT_LEAF_OFFSET]))
}

func create_new_root(table *Table, rightChildPageNum uint32) {
	/*
		Handling splitting the root.
		Old root copied to new page, becomes left child.
		Address of right child passed in.
		Re-initialize root page to contain the new root node.
		New root node points to two children.
	*/
	root := get_page(table.pager, table.rootPageNum)
	leftChildPageNum := get_unused_page_num(table.pager)
	leftChildPage := get_page(table.pager, leftChildPageNum)

	/* Left child has data copied from old root */
	copy(leftChildPage, root)
	set_node_root(leftChildPage, false)

	/* Root node is a new internal node with one key and two children */
	initialize_internal_node(root)
	set_node_root(root, true)
	*internal_node_num_keys(root) = 1
	*internal_node_child(root, 0) = leftChildPageNum
	leftChildMaxKey := get_node_max_key(leftChildPage)
	*internal_node_cell_key(root, 0) = leftChildMaxKey
	*internal_node_right_child(root) = rightChildPageNum
}

func internal_node_num_keys(node []byte) *uint32 {
	return (*uint32)(unsafe.Pointer(&node[INTERNAL_NODE_NUM_KEYS_OFFSET]))
}

/*
 * Return the page num of the right most child
 */
func internal_node_right_child(node []byte) *uint32 {
	return (*uint32)(unsafe.Pointer(&node[INTERNAL_NODE_RIGHT_CHILD_OFFSET]))
}

/*
 * Return the page num based on the cell num
 */
func internal_node_cell_value(node []byte, cellNum uint32) *uint32 {
	return (*uint32)(unsafe.Pointer(&node[INTERNAL_NODE_HEADER_SIZE+cellNum*INTERNAL_NODE_CELL_SIZE]))
}

/*
 * Return the page num based on the chlid num
 */
func internal_node_child(node []byte, childNum uint32) *uint32 {
	numKeys := *internal_node_num_keys(node)
	if childNum > numKeys {
		fmt.Printf("Tried to access childNum %d > numKeys %d\n", childNum, numKeys)
		os.Exit(1)
	} else if childNum == numKeys {
		return internal_node_right_child(node)
	} else {
		return internal_node_cell_value(node, childNum)
	}
	return nil
}

func internal_node_cell(node []byte, cellNum uint32) []byte {
	offset := INTERNAL_NODE_HEADER_SIZE + cellNum*INTERNAL_NODE_CELL_SIZE
	return node[offset : offset+INTERNAL_NODE_CELL_SIZE]
}

func internal_node_cell_key(node []byte, keyNum uint32) *uint32 {
	offset := INTERNAL_NODE_HEADER_SIZE + keyNum*INTERNAL_NODE_CELL_SIZE + INTENRAL_NODE_CHILD_SIZE
	return (*uint32)(unsafe.Pointer(&node[offset]))
}

func get_node_max_key(node []byte) uint32 {
	switch get_node_type(node) {
	case (NODE_INTERNAL):
		return *internal_node_cell_key(node, *internal_node_num_keys(node)-1)
	case (NODE_LEAF):
		return *leaf_node_cell_key(node, *leaf_node_num_cells(node)-1)
	default:
		fmt.Printf("The node type isn't supported\n")
		os.Exit(1)
		return 0
	}
}

func is_node_root(node []byte) bool {
	value := uint32(node[IS_ROOT_OFFSET])
	return value != 0
}

func set_node_root(node []byte, isRoot bool) {
	if isRoot {
		node[IS_ROOT_OFFSET] = byte(1)
	} else {
		node[IS_ROOT_OFFSET] = byte(0)
	}
}

func node_parent(node []byte) *uint32 {
	return (*uint32)(unsafe.Pointer(&node[PARENT_POINTER_OFFSET]))
}

func update_internal_node_key(node []byte, oldKey uint32, newKey uint32) {
	oldChildIndex := internal_node_find_child(node, oldKey)
	*internal_node_cell_key(node, oldChildIndex) = newKey
}

func internal_node_insert(table *Table, parentPageNum uint32, childPageNum uint32) {
	/*
	 * Add a new child/key pair to parent that corresponds to child
	 */

	parent := get_page(table.pager, parentPageNum)
	child := get_page(table.pager, childPageNum)
	childMaxKey := get_node_max_key(child)
	index := internal_node_find_child(parent, childMaxKey)

	originalNumKeys := *internal_node_num_keys(parent)
	*internal_node_num_keys(parent) = originalNumKeys + 1

	if originalNumKeys >= INTERNAL_NODE_MAX_CELLS {
		fmt.Printf("Need to implement splitting internal node\n")
		os.Exit(1)
	}

	rightChildPageNum := *internal_node_right_child(parent)
	rightChild := get_page(table.pager, rightChildPageNum)

	if childMaxKey > get_node_max_key(rightChild) {
		/* Replace the right child */
		*internal_node_child(parent, originalNumKeys) = rightChildPageNum
		*internal_node_cell_key(parent, originalNumKeys) = get_node_max_key(rightChild)
		*internal_node_right_child(parent) = childPageNum
	} else {
		/* Make room for the new cell */
		for i := originalNumKeys; i > index; i-- {
			dest := internal_node_cell(parent, i)
			src := internal_node_cell(parent, i-1)
			copy(dest, src)
		}
		*internal_node_child(parent, index) = childPageNum
		*internal_node_cell_key(parent, index) = childMaxKey
	}
}
