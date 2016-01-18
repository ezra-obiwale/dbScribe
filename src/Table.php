<?php

namespace DBScribe;

use Exception;

/**
 * This class holds all information concerning a database table and methods to operate
 * on the table and it's columns and rows
 *
 * @author Ezra Obiwale <contact@ezraobiwale.com>
 */
class Table {

    const ORDER_ASC = 'ASC';
    const ORDER_DESC = 'DESC';
    const INDEX_REGULAR = 'INDEX';
    const INDEX_UNIQUE = 'UNIQUE';
    const INDEX_FULLTEXT = 'FULLTEXT';
    const OP_SELECT = 'select';
    const OP_INSERT = 'insert';
    const OP_UPDATE = 'update';
    const OP_DELETE = 'delete';
    const RETURN_DEFAULT = 0;
    const RETURN_MODEL = 1;
    const RETURN_JSON = 2;

    /**
     * Table name
     * @var string
     */
    protected $name;

    /**
     * Connection object
     * @var Connection
     */
    protected $connection;

    /**
     * Table Description
     * @var string
     */
    protected $description;

    /**
     * New Table Description
     * @var string
     */
    protected $newDescription;

    /**
     * Array of columns and some of their properties
     * @var array
     */
    protected $columns;

    /**
     * Array of new columns and their definitions
     * @var array
     */
    protected $newColumns;

    /**
     * Array of references from this table to other tables
     * @var array
     */
    protected $references;

    /**
     * Array of references from other tables to this table
     * @var array
     */
    protected $backReferences;

    /**
     * Array of indexes
     * @var string
     */
    protected $indexes;

    /**
     * Array of new references from this table to others
     * @var array
     */
    protected $newReferences;

    /**
     * The primary key of the table
     * @var string
     */
    protected $primaryKey;

    /**
     * Indicates whether to remove the primary key
     * @var boolean
     */
    protected $dropPrimaryKey;

    /**
     * The new primary key to replace the old
     * @var string
     */
    protected $newPrimaryKey;

    /**
     * Array of columns to remove from the table
     * @var array
     */
    protected $dropColumns;

    /**
     * Array of existing columns with new definitions
     * @var array
     */
    protected $alterColumns;

    /**
     * Array of columns whose references should be dropped
     * @var array
     */
    protected $dropReferences;

    /**
     * Array of columns whose references need be changed
     * @var array
     */
    protected $alterReferences;

    /**
     * Array of columns to add indexes to
     * @var array
     */
    protected $newIndexes;

    /**
     * Array of columns whose indexes should be dropped
     * @var array
     */
    protected $dropIndexes;

    /**
     * The query string to be executed
     * @var string
     */
    protected $query;

    /**
     * The last query executed
     * @var string
     */
    protected $lastQuery;

    /**
     * Columns to target the query
     * @var Array|String
     */
    protected $targetColumns;

    /**
     * The query that serves to join results with referenced tables' rows
     * @var string
     */
    protected $joinQuery;

    /**
     * Array of prepared values to be saved to the database
     * @var array
     */
    protected $values;

    /**
     * Array of referenced tables from which to draw more rows
     * @var array
     */
    protected $joins;

    /**
     * Indicates whether multiple values are prepared for the query
     * @var boolean
     */
    protected $multiple;

    /**
     * Indicates whether to return results with a class model
     * @var boolean
     */
    protected $withModel;

    /**
     * The class that extends \DBScribe\Row which to map results to
     * @var Row
     */
    protected $rowModel;

    /**
     * Holds the current model the class is working with
     * @var Row
     */
    protected $rowModelInUse;

    /**
     * Array of columns and options to order the result by
     * @var array
     */
    protected $orderBy;

    /**
     * The limit part of the query
     * @var string
     */
    protected $limit;

    /**
     * Indicates whether to delay the execution of the query until @method execute() is called
     * @var boolean
     */
    protected $delayExecute;

    /**
     * Indicates whether to run the postSave method of the Row after operating the query
     * @var boolean
     */
    protected $doPost;

    /**
     * Currenct operation: one of the OP_ constants of this class
     * @var string
     */
    protected $current;

    /**
     * Additional conditions to attach to the query
     * @var string
     */
    protected $customWhere;

    /**
     * Conditions to attach to the query
     * @var string
     */
    protected $where;

    /**
     * Array of columns to group query results by
     * @var array
     */
    protected $groups;

    /**
     * The having portion of the query
     * @var string
     */
    protected $having;

    /**
     * Array holding the relationship information with all other tables
     * @var array
     */
    protected $relationshipData;

    /**
     * Used with customWhere(). No relationship with join()
     * @var string AND|OR
     */
    protected $customWhereJoin;

    /**
     * Indicates the type of results expected
     * @var int One of the RETURN_* constants of this class
     */
    protected $expected;

    /**
     * Indicates whether to retrieve data from cache or not
     * @var bool
     */
    protected $fromCache;

    /**
     * Indicates whether the query has been generated
     * @var boolean
     */
    protected $genQry;

    /**
     * Indicates whether to preserve queries that change the table or not
     * @var boolean
     */
    protected $preserveQueries;

    /**
     * Allows reuse of parameters
     * @var boolean
     */
    protected $reuseParams;

    /**
     * Class contructor
     * @param string $name Name of the table, without the prefix if already
     * supplied in the connection object
     * @param Connection $connection
     * @param Row $rowModel
     */
    public function __construct($name, Connection $connection = null,
            Row $rowModel = null) {
        $this->name = $connection->getTablePrefix() . strtolower(Util::camelTo_($name));
        $this->connection = $connection;
        $this->rowModel = ($rowModel) ? $rowModel : new Row();
        $this->multiple = false;
        $this->doPost = false;
        $this->delayExecute = false;
        $this->where = null;
        $this->groups = array();
        $this->orderBy = array();
        $this->joins = array();
        $this->fromCache = true;
        $this->genQry = false;

        $this->columns = array();
        $this->references = array();
        $this->backReferences = array();
        $this->indexes = array();
        $this->foreignKeys = array();

        $this->newColumns = array();
        $this->newReferences = array();
        $this->alterColumns = array();
        $this->dropColumns = array();
        $this->dropReferences = array();
        $this->dropIndexes = array();
        $this->alterReferences = array();

        $this->init();
        $this->checkDATA();
    }

    /**
     * Checks if the DATA constant is declared and declares it if not
     * @return Table
     */
    public function checkDATA() {
        if (!defined(DATA))
                define(DATA,
                    __DIR__ . DIRECTORY_SEPARATOR . 'data' . DIRECTORY_SEPARATOR);

        if (!is_dir(DATA)) mkdir(DATA, 0777, true);

        return $this;
    }

    /**
     * Indicates whether to preserve queries that change the table or not
     * @param bool $bool
     * @return \DBScribe\Table
     */
    public function preserveQueries($bool = true) {
        $this->preserveQueries = $bool;
        return $this;
    }

    protected function doPreserveQueries() {
        $path = DATA . md5('queries') . DIRECTORY_SEPARATOR;
        if (!is_dir($path)) mkdir($path, 0777, true);
        $today = Util::createTimestamp(time(), 'Y-m-d');
        $save = array();
        if (is_readable($path . $today)) {
            $save = include $path . $today;
        }
        $save[] = array(
            'q' => $this->query,
            'v' => $this->values,
        );
        Util::updateConfig($path . $today, $save);
    }

    /**
     * Sets the model to use with fetched rows
     * @param Row $model
     * @return Table
     */
    public function setRowModel(Row $model) {
        $this->rowModel = $model;
        return $this;
    }

    /**
     * Fetches the model attached to the table
     * @return Row $model
     */
    public function getRowModel() {
        return $this->rowModel;
    }

    /**
     * Fetches the connection used in the table
     * @return Connection
     */
    public function getConnection() {
        return $this->connection;
    }

    /**
     * Initialiazes the table
     */
    public function init() {
        if ($this->connection) $this->defineRelationships();
    }

    /**
     * Fetches the name of the table
     * @return string
     */
    public function getName() {
        return $this->name;
    }

    /**
     * Set the table description
     * @param string $tableDescription
     * @return Table
     */
    public function setDescription($tableDescription = 'ENGINE=InnoDB') {
        if (!$this->description) $this->description = $tableDescription;
        return $this;
    }

    /**
     * Change the table description
     * @param string $tableDescription
     * @return Table
     */
    public function changeDescription($tableDescription = 'ENGINE=InnoDB') {
        $this->newDescription = $tableDescription;
        return $this;
    }

    /**
     * Fetches the description of the table
     * @return string|null
     */
    public function getDescription() {
        return $this->description;
    }

    /**
     * Fetches the new description for the table
     * @return string|null
     */
    public function getNewDescription($reset = false) {
        $return = $this->newDescription;
        if ($reset) $this->newDescription = null;
        return $return;
    }

    /**
     * Sets the primary key
     * @param string $pk
     * @return Table
     */
    public function setPrimaryKey($pk) {
        if ($this->primaryKey === $pk) return $this;

        if ($this->primaryKey) {
            $this->dropPrimaryKey();
        }
        $this->newPrimaryKey = $pk;

        return $this;
    }

    /**
     * Fetches the primary key
     * @return string|null
     */
    public function getPrimaryKey() {
        return $this->primaryKey;
    }

    /**
     * Removes the primary key
     * @return Table
     */
    public function dropPrimaryKey() {
        $this->dropPrimaryKey = true;
        return $this;
    }

    /**
     * Checks if to drop the primary key
     * @return boolean|null
     */
    public function shouldDropPrimaryKey($reset = false) {
        $return = $this->dropPrimaryKey;
        if ($reset) $this->dropPrimaryKey = false;
        return $return;
    }

    /**
     * Fetches the new primary key
     * @return string|null
     */
    public function getNewPrimarykey($reset = false) {
        $return = $this->newPrimaryKey;
        if ($reset) $this->newPrimaryKey = null;
        return $return;
    }

    /**
     * Fetches the indexes
     * @param string $columnName Name of column to return the index
     * @return array
     */
    public function getIndexes($columnName = null) {
        if (!$this->indexes) {
            $this->indexes = array();
        }
        return ($columnName) ? $this->indexes[$columnName] : $this->indexes;
    }

    /**
     * Adds an index to a column
     * @param string $columnName
     * @param string $type Should be one of \DBScribe\Table::INDEX_REGULAR,  \DBScribe\Table::INDEX_UNIQUE,
     * or  \DBScribe\Table::INDEX_FULLTEXT
     * @return Table
     */
    public function addIndex($columnName, $type = Table::INDEX_REGULAR) {
        if (!array_key_exists($columnName, $this->getIndexes()) && !array_key_exists($columnName,
                        $this->newIndexes))
                $this->newIndexes[$columnName] = $type;
        return $this;
    }

    /**
     * Fetches the indexes to create
     * @return array
     */
    public function getNewIndexes($reset = false) {
        $return = $this->newIndexes;
        if ($reset) $this->newIndexes = array();
        return $return;
    }

    /**
     * Removes index from a column
     * @param string $columnName
     * @return Table
     */
    public function dropIndex($columnName) {
        if (!in_array($columnName, $this->dropIndexes))
                $this->dropIndexes[] = $columnName;

        if (array_key_exists($columnName, $this->getReferences()))
                $this->dropReference($columnName);
        return $this;
    }

    /**
     * Fetches the indexes to remove
     * @param bool $reset Reset the indexes
     * @return array
     */
    public function getDropIndexes($reset = false) {
        $return = $this->dropIndexes;
        if ($reset) $this->dropIndexes = array();
        return $return;
    }

    /**
     * Add a column to the table
     * @param string $columnName
     * @param string $columnDescription
     * @return Table
     */
    public function addColumn($columnName, $columnDescription) {
        $this->newColumns[$columnName] = $columnDescription;
        return $this;
    }

    /**
     * Gets available columns in table
     * @return array
     */
    public function getColumns($justNames = false) {
        return ($justNames) ? array_keys($this->columns) : $this->columns;
    }

    /**
     * Removes a column
     * @param string $columnName
     * @return Table
     */
    public function dropColumn($columnName) {
        $this->dropColumns[] = $columnName;
        if (array_key_exists($columnName, $this->references)) {
            $this->dropReference($columnName);
        }
        return $this;
    }

    /**
     * Fetches columns to remove
     * @param bool $reset Reset the columns
     * @return array
     */
    public function getDropColumns($reset = false) {
        $return = $this->dropColumns;
        if ($reset) $this->dropColumns = array();
        return $return;
    }

    /**
     * Fetches columns to add
     * @param bool $reset Reset the columns
     * @return array
     */
    public function getNewColumns($reset = false) {
        $return = $this->newColumns;
        if ($reset) $this->newColumns = array();
        return $return;
    }

    /**
     * Alter column description
     * @param string $columnName
     * @param string $columnDescription
     * @return Table
     */
    public function alterColumn($columnName, $columnDescription) {
        $this->alterColumns[$columnName] = $columnDescription;
        return $this;
    }

    /**
     * Fetches the columns to change
     * @return array
     */
    public function getAlterColumns($reset = false) {
        $return = $this->alterColumns;
        if ($reset) $this->alterColumns = array();
        return $return;
    }

    /**
     * Fetches the references in the table
     * @return array
     */
    public function getReferences() {
        return $this->references;
    }

    /**
     * Fetches the tables and columns that reference this table
     * @return array
     */
    public function getBackReferences() {
        return $this->backReferences;
    }

    /**
     * Removes a reference from a column
     * @param string $columnName
     * @return Table
     */
    public function dropReference($columnName) {
        $this->dropReferences[] = $columnName;
        return $this;
    }

    /**
     * Fetches all columns from which references should be dropped
     * @return array
     */
    public function getDropReferences($reset = false) {
        $return = array_unique($this->dropReferences);
        if ($reset) $this->dropReferences = array();
        return $return;
    }

    /**
     * Add reference to a column
     * @param string $columnName
     * @param string $refTable
     * @param string $refColumn
     * @return Table
     */
    public function addReference($columnName, $refTable, $refColumn,
            $onDelete = 'RESTRICT', $onUpdate = 'RESTRICT') {
        $this->addIndex($columnName);
        $this->newReferences[$columnName] = array(
            'table' => $refTable,
            'column' => $refColumn,
            'onDelete' => $onDelete,
            'onUpdate' => $onUpdate,
        );
        return $this;
    }

    /**
     * Fetches all new references
     * @return array
     */
    public function getNewReferences($reset = false) {
        $return = $this->newReferences;
        if ($reset) $this->newReferences = array();
        return $return;
    }

    /**
     * Alter references of the table column
     * @param string $columnName
     * @param string $refTable
     * @param string $refColumn
     * @return Table
     */
    public function alterReference($columnName, $refTable, $refColumn,
            $onDelete = 'RESTRICT', $onUpdate = 'RESTRICT') {
        $this->dropReference($columnName);
        $this->addReference($columnName, $refTable, $refColumn, $onDelete,
                $onUpdate);
        return $this;
    }

    /**
     * Sets the model to map the table to
     * @param Row $model
     * @return Table
     */
    public function setModel(Row $model) {
        $this->rowModel = $model;
        return $this;
    }

    /**
     * Fetches the model set for the table
     * @return Row
     */
    public function getModel() {
        return $this->rowModel;
    }

    /**
     * Defines the relationships of the table
     * @return void
     */
    private function defineRelationships() {
        $this->fetchColumns();
        $this->fetchReferences();
        $this->fetchBackReferences();
    }

    /**
     * Fetches the tables that reference this table, and their columns
     */
    private function fetchBackReferences() {
        $qry = "SELECT k.COLUMN_NAME as refColumn, k.TABLE_SCHEMA as refDB, k.TABLE_NAME as refTable,
			k.REFERENCED_COLUMN_NAME as columnName" .
                " FROM information_schema.KEY_COLUMN_USAGE k" .
                " WHERE k.TABLE_SCHEMA = '" . $this->connection->getDBName() .
                "' AND k.REFERENCED_TABLE_NAME = '" . $this->name . "'";

        $backRef = $this->connection->doPrepare($qry);
        if (is_bool($backRef)) return;

        foreach ($backRef as &$info) {
            $name = $info['columnName'];
            unset($info['columnName']);
            $this->backReferences[$name][] = $info;
        }
    }

    /**
     * Fetches all tables and columns that this table references
     */
    private function fetchReferences() {
        $qry = "SELECT i.CONSTRAINT_NAME as constraintName, i.CONSTRAINT_TYPE as constraintType,
			j.COLUMN_NAME as columnName, j.REFERENCED_TABLE_SCHEMA as refDB, j.REFERENCED_TABLE_NAME as refTable,
			j.REFERENCED_COLUMN_NAME as refColumn, k.UPDATE_RULE as onUpdate, k.DELETE_RULE as onDelete" .
                " FROM information_schema.TABLE_CONSTRAINTS i" .
                " LEFT JOIN information_schema.KEY_COLUMN_USAGE j
                    ON i.CONSTRAINT_NAME = j.CONSTRAINT_NAME AND j.TABLE_SCHEMA = '" . $this->connection->getDBName() . "'
			AND j.TABLE_NAME = '" . $this->name . "'" .
                " LEFT JOIN information_schema.REFERENTIAL_CONSTRAINTS k
                    ON i.CONSTRAINT_NAME = k.CONSTRAINT_NAME AND j.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA
			AND k.TABLE_NAME = '" . $this->name . "'" .
                " WHERE i.TABLE_SCHEMA = '" . $this->connection->getDBName() . "'
								AND i.TABLE_NAME = '" . $this->name . "'";

        $define = $this->connection->doPrepare($qry);
        if (is_bool($define)) return;

        foreach ($define as $info) {
            if (isset($info['constraintType']) && $info['constraintType'] === 'PRIMARY KEY') {
                if (isset($info['columnName']))
                        $this->primaryKey = $info['columnName'];
            } else if ($info['refTable']) {
                if (isset($info['constraintType']))
                        unset($info['constraintType']);
                if (isset($info['columnName'])) {
                    $name = $info['columnName'];
                    unset($info['columnName']);
                }
                $this->references[$name] = $info;
            }
        }
    }

    /**
     * Fetches all columns of the table and their information
     */
    private function fetchColumns() {
        $qry = 'SELECT c.column_name as colName, c.column_default as colDefault,
			c.is_nullable as nullable, c.column_type as colType, c.extra, c.column_key as colKey,
                        c.character_set_name as charset, c.collation_name as collation';
        $qry .= ', d.index_name as indexName';
        $qry .=' FROM INFORMATION_SCHEMA.COLUMNS c ' .
                'LEFT JOIN INFORMATION_SCHEMA.STATISTICS d'
                . ' ON c.column_name = d.column_name AND d.table_schema="' . $this->connection->getDBName() . '" AND d.table_name="' . $this->name . '" ' .
                'WHERE c.table_schema="' . $this->connection->getDBName() . '" AND c.table_name="' . $this->name . '"';

        $columns = $this->connection->doPrepare($qry);
        if (is_bool($columns)) return;

        foreach ($columns as $column) {
            $this->columns[$column['colName']] = $column;
            if (in_array($column['colKey'],
                            array('MUL', 'UNI', 'PRI', 'SPA', 'FUL'))) {
                $this->indexes[$column['colName']] = $column['indexName'];
            }
        }
    }

    public function getConstraintName($column) {
        if (array_key_exists($column, $this->references)) {
            return $this->references[$column]['constraintName'];
        }

        return null;
    }

    /**
     * Checks if the table exists
     * @return boolean
     */
    public function exists() {
        return (count($this->columns));
    }

    /**
     * Checks if a connection and table exist
     * @return boolean
     * @throws Exception
     */
    private function checkExists() {
        if (!$this->connection)
                throw new Exception('Invalid action. No connection found');

        return $this->exists();
    }

    /**
     * Inserts the given row(s) into the table<br />
     * Many rows can be inserted at once.
     * @param array $values Array with values \DBScribe\Row or array of [column => value]
     * @return Table
     */
    public function insert(array $values) {
        if (!$this->checkExists()) return false;

        $this->current = self::OP_INSERT;
        $this->query = 'INSERT INTO `' . $this->name . '` (';
        $columns = array();
        $noOfColumns = 0;
        foreach (array_values($values) as $ky => $row) {
            $rowArray = $this->checkModel($row, true);
            if ($ky === 0) $noOfColumns = count($rowArray);

            if (count($rowArray) !== $noOfColumns) {
                throw new Exception('All rows must have the same number of columns in table "' . $this->name .
                '". Set others as null');
            }

            if (count($rowArray) === 0)
                    throw new Exception('You cannot insert an empty row into table "' . $this->name . '"');

            foreach ($rowArray as $column => &$value) {
                if (empty($value) && $value != 0) continue;

                $column = Util::camelTo_($column);

                if (!in_array($column, $columns)) $columns[] = $column;
                $this->values[$ky][':' . $column] = $value;
            }
        }

        $this->query .= '`' . join('`, `', $columns) . '`';
        $this->query .= ') VALUES (';
        $this->query .= ':' . join(', :', $columns);
        $this->query .= ')';

        $this->multiple = true;
        $this->doPost = self::OP_INSERT;
        if ($this->delayExecute) {
            return $this;
        }

        return $this->execute();
    }

    /**
     * Fetches the relationships between the columns in this table and the
     * give table
     * @param string $table
     * @return array
     */
    public function getTableRelationships($table) {
        $table = $this->connection->getTablePrefix() . Util::camelTo_($table);
        $relationships = array();
        foreach ($this->references as $columnName => $info) {
            if ($info['constraintName'] == 'PRIMARY' || $info['refTable'] != $table)
                    continue;
            $this->setupRelationship($columnName, $info['refColumn'],
                    $info['refTable'], $relationships, false);
        }
        foreach ($this->backReferences as $columnName => $infoArray) {
            foreach ($infoArray as $info) {
                if ($info['refTable'] != $table) continue;
                $this->setupRelationship($columnName, $info['refColumn'],
                        $info['refTable'], $relationships, true);
            }
        }
        return $relationships[$table];
    }

    /**
     * Fetches the relationships between the specified column and other columns
     * (in other tables)
     * @param string $columnName
     * @return array
     */
    public function getColumnRelationships($columnName) {
        $columnName = Util::camelTo_($columnName);
        $relationships = array();
        if ($info = $this->references[$columnName]) {
            if ($info['constraintName'] != 'PRIMARY' && !empty($info['refTable'])) {
                $this->setupRelationship($columnName, $info['refColumn'],
                        $info['refTable'], $relationships, false);
            }
        }
        if ($info = $this->backReferences[$columnName]) {
            if (!empty($info['refTable'])) {
                $this->setupRelationship($columnName, $info['refColumn'],
                        $info['refTable'], $relationships, true);
            }
        }

        return $relationships;
    }

    private function setupRelationship($columnName, $refColumn, $refTable,
            array &$relationships, $push = true) {
        return $relationships[$refTable][] = array(
            'column' => $columnName,
            'refColumn' => $refColumn,
            'push' => $push
        );
    }

    private function prepareColumns(Table $table = null, $alias = null) {
        $ignoreJoins = false;
        if (!$table) {
            $table = $this;
            $ignoreJoins = true;
        }
        $return = '';
        if (!$table->hasTargetColumns() && $table->getModel() !== null && count($table->getModel()->toArray())) {
            $table->targetColumns(array_keys($table->getRowModel()->toArray(true)));
        }
        else if (!$table->hasTargetColumns()) {
            $table->targetColumns($table->getColumns(true));
        }

        foreach ($table->targetColumns as &$column) {
            $column = Util::camelTo_(trim($column));
            if ($return) $return .= ', ';

            $return .= '`' . (($alias) ? $alias : $table->getName()) . '`.`' . $column . '`';
            if ($this->joins && !$ignoreJoins) {
                $return .= ' as ' . Util::_toCamel($table->getName()) . '_' . Util::_toCamel($column);
            }
            else if ($ignoreJoins) {
                $return .= ' as ' . Util::_toCamel($column);
            }
        }
        return $return;
    }

    /**
     * This targets the query at the given columns
     * @param array|string $columns Array or comma-separated string of columns
     * @return Table
     */
    public function targetColumns($columns) {
        $this->targetColumns = is_array($columns) ? $columns : explode(',',
                        $columns);
        return $this;
    }

    public function hasTargetColumns() {
        return !is_null($this->targetColumns);
    }

    /**
     * Selects the given columns from rows with the given criteria
     * Many rows can be passed in as criteria
     * @param array|string $columns Array or comma-separated string of columns
     * @param array $criteria Array with values \DBScribe\Row or array of [column => value]
     * @param int $return Indicates the type of result expected
     * @return Table|ArrayCollection
     */
    public function selectColumns($columns, array $criteria = array(),
            $return = Table::RETURN_MODEL) {
        $this->targetColumns($columns);
        return $this->select($criteria, $return);
    }

    /**
     * Selects rows from database
     * Many rows can be passed in as criteria
     * @param array $criteria Array with values \DBScribe\Row or array of [column => value]
     * @param int $return Indicates the type of result expected
     * @return Table|ArrayCollection
     */
    public function select(array $criteria = array(),
            $return = Table::RETURN_MODEL) {
        if (!$this->checkExists()) {
            return ($this->delayExecute) ? $this : new ArrayCollection();
        }

        $this->current = self::OP_SELECT;

        $this->query = 'SELECT ' . $this->prepareColumns();
        $this->query .= ' FROM `' . $this->name . '`' . $this->processJoins();

        $this->where($criteria);

        $this->setExpectedResult($return, true);
        if ($this->delayExecute) {
            return $this;
        }
        return $this->execute();
    }

    /**
     * Fetches the results from cache if available
     * @return array|nul
     */
    private function getCached() {
        if (isset($_GET['noCache'])) return null;
        $cacheDir = DATA . 'select' . DIRECTORY_SEPARATOR;
        if (!is_dir($cacheDir)) mkdir($cacheDir, 0777, true);

        $cache = $cacheDir . base64_encode($this->getName()) . '.php';
        if (!is_readable($cache)) return null;
        $cached = include $cache;
        return $this->decode($cached[$this->encode($this->query . serialize($this->values))]);
    }

    /**
     * Saves the given result, if valid, to cache for future uses
     * @param array $result
     * @return boolean
     */
    private function saveCache($result) {
        if (!$result) return false;
        $cache = DATA . 'select' . DIRECTORY_SEPARATOR . $this->encode($this->getName()) . '.php';
        return Util::updateConfig($cache,
                        array($this->encode($this->query . serialize($this->values)) => $this->encode(serialize($result))));
    }

    /**
     * Determines whether to retrieve data from cache or not.
     * @param bool $bool
     * @return \DBScribe\Table
     */
    public function fromCache($bool = true) {
        $this->fromCache = $bool;
        return $this;
    }

    /**
     * Checks whether the returned data is gotten from cache or not
     * @return bool
     */
    public function isCached() {
        return $this->fromCache;
    }

    /**
     * Removes cached result because the table has been updated
     * @return bool
     */
    private function removeCache() {
        return unlink(DATA . 'select' . DIRECTORY_SEPARATOR . $this->encode($this->getName()) . '.php');
    }

    /**
     * Encodes the given data
     * @param string $data
     * @return string
     */
    private function encode($data) {
        return base64_encode($data);
    }

    /**
     * Decodes the given data and unserializes it
     * @param string $data
     * @return array|bool False if not unserializable. Array otherwise
     */
    private function decode($data) {
        return unserialize(base64_decode($data));
    }

    /**
     * Sets the type of result expected
     * @param int $expected One of \DBScribe\Table::RETURN_DEFAULT,
     * \DBScribe\Table::RETURN_MODEL or \DBScribe\Table::RETURN_JSON
     * @param bool $checkNotSet Only set if not already
     * @return Table
     */
    public function setExpectedResult($expected = Table::RETURN_MODEL,
            $checkNotSet = false) {
        if (!$checkNotSet || ($checkNotSet && is_null($this->expected)))
                $this->expected = $expected;
        return $this;
    }

    /**
     * Create the where part of the query
     * @param array $criteria Array of row column array
     * @param bool $joinWithAnd Indicates whether to join the query with the
     * previous one with the logical AND
     * @param bool $notEqual Indicates whether not to equate the giveM value to actual
     * column value
     * @param boolean $valuesAreColumns Indicates whether the criteria values are columnNames
     * @return Table
     */
    public function where(array $criteria, $joinWithAnd = true,
            $notEqual = false, $valuesAreColumns = false) {
        if (count($criteria)) {
            if (!$this->where) {
                $this->where = ' WHERE (';
                $opened = true;
            }
//            else if ($this->where && substr($this->where,
//                            strlen($this->where) - 1) === ')')
//                    $this->where = substr($this->where, 0,
//                        strlen($this->where) - 1);

            if (substr($this->where, strlen($this->where) - 1) != '(') {
                $this->where .= $joinWithAnd ? ' AND ' : ' OR ';
            }
            foreach ($criteria as $ky => $row) {
                if ($ky) $this->where .= ' OR (';

                $rowArray = $this->checkModel($row);
                $cnt = 1;
                foreach ($rowArray as $column => $value) {
                    if (!is_array($value)) {
                        $this->where .= '`' . $this->name . '`.`' . Util::camelTo_($column) . '` ';
                        $this->where .= ($notEqual) ? '!=' : '=';
                        if ($valuesAreColumns)
                                $this->where .= '`' . $this->name . '`.`' . Util::camelTo_($value) . '` ';
                        else {
                            $this->where .= ' ?';
                            $this->values[] = $value;
                        }
                        if (count($rowArray) > $cnt) $this->where .= ' AND ';
                    }
                    else {
                        $this->where .= '`' . $this->name . '`.`' . Util::camelTo_($column) . '` ';
                        $this->where .= ($notEqual) ? 'NOT IN' : 'IN';
                        $this->where .= ' (';
                        if ($valuesAreColumns) {
                            $n = 0;
                            foreach ($value as $val) {
                                if ($n) $this->where .= ', ';
                                $this->where .= '`' . $this->name . '`.`' .
                                        Util::camelTo_($val) . '`';
                                $n++;
                            }
                        }
                        else {
                            $this->where .= '?' . str_repeat(',?',
                                            count($value) - 1);
                            $this->values = $this->values ? array_merge($this->values,
                                            $value) : $value;
                        }
                        $this->where .= ')';
                    }
                    $cnt++;
                }
                if ($opened) $this->where .= ')';
            }
        }
        return $this;
    }

    /**
     * Check if the intending query has conditions to go with it
     * @return boolean
     */
    public function hasCondition() {
        return ($this->where || $this->customWhere);
    }

    /**
     * Sets a column as the key to hold each result. Default is ID. This is only
     * valid if return type IS NOT MODEL
     * @param string $column
     * @return \DBScribe\Table
     */
    public function setResultKey($column) {
        $this->resultKey = $column;
        return $this;
    }

    private function returnSelect($return) {
        if (!is_array($return)) {
            $return = array();
        }
        $forThis = $this->relationshipData = array();

        foreach ($return as &$ret) {
            $imm = array();
            foreach ($this->targetColumns as $col) {
                $imm[Util::_toCamel($col)] = @$ret[Util::_toCamel($col)];
                unset($ret[Util::_toCamel($col)]);
            }

            if ($this->resultKey && !empty($imm[Util::_toCamel($this->resultKey)])) {
                $forThis[$imm[Util::_toCamel($this->resultKey)]] = $imm;
            }
            else if ($this->getPrimaryKey() && !empty($imm[Util::_toCamel($this->getPrimaryKey())])) {
                $forThis[$imm[Util::_toCamel($this->getPrimaryKey())]] = $imm;
            }
            else $forThis[] = $imm;

            if (!empty($ret)) {
                $this->relationshipData[] = $ret;
            }
        }
        switch ($this->expected) {
            case self::RETURN_JSON:
                $return = json_encode($forThis);
                break;
            case self::RETURN_MODEL:
                $return = $this->createReturnModels($forThis);
                break;
            case self::RETURN_DEFAULT:
                $return = $forThis;
                break;
        }
        return $return;
    }

    private function createReturnModels(array $forThis) {
        $rows = new ArrayCollection();

        foreach ($forThis as $valueArray) {
            $row = clone $this->rowModel;
            foreach ($valueArray as $name => $value) {
                if (method_exists($row, 'set' . $name))
                        $row->{'set' . $name}($value);
                else $row->{$name} = $value;
            }
            $row->postFetch();
            $row->setTable($this);
            $rows->append($row);
        }

        return $rows;
    }

    /**
     * Select a column where it is LIKE the value, i.e. it contains the given
     * value     *
     * @param string $column
     * @param mixed $value
     * @param boolean $logicalAnd Indicates whether to use logical AND (TRUE) or OR (FALSE)
     * @return Table
     */
    public function like($column, $value, $logicalAnd = true) {
        $this->customWhere('`:TBL:`.`' . Util::camelTo_($column) . '` LIKE "' . $value . '"',
                $logicalAnd ? 'AND' : 'OR');
        return $this;
    }

    /**
     * Select a column where it is NOT LIKE the value, i.e. it does not contain
     *  the given value
     * @param string $column
     * @param mixed $value
     * @param boolean $logicalAnd Indicates whether to use logical AND (TRUE) or OR (FALSE)
     * @return Table
     */
    public function notLike($column, $value, $logicalAnd = true) {
        $this->customWhere('`:TBL:`.`' . Util::camelTo_($column) . '` NOT LIKE "' . $value . '"',
                $logicalAnd ? 'AND' : 'OR');
        return $this;
    }

    /**
     * Select a column where it is less than the given value
     * @param string $column
     * @param mixed $value
     * @param boolean $logicalAnd Indicates whether to use logical AND (TRUE) or OR (FALSE)
     * @param boolean $valueIsColumn Indicates whether the value is another columnName
     * @return Table
     */
    public function lessThan($column, $value, $logicalAnd = true,
            $valueIsColumn = false) {
        $value = $valueIsColumn ? '`:TBL:`.`' . \Util::camelTo_($value) . '`' :
                '"' . $value . '"';
        $this->customWhere('`:TBL:`.`' . Util::camelTo_($column) . '` < ' . $value,
                $logicalAnd ? 'AND' : 'OR');
        return $this;
    }

    /**
     * Select a column where it is less than or equal to the given value
     * @param string $column
     * @param mixed $value
     * @param boolean $logicalAnd Indicates whether to use logical AND (TRUE) or OR (FALSE)
     * @param boolean $valueIsColumn Indicates whether the value is another columnName
     * @return Table
     */
    public function lessThanOrEqualTo($column, $value, $logicalAnd = true,
            $valueIsColumn = false) {
        $value = $valueIsColumn ? '`:TBL:`.`' . \Util::camelTo_($value) . '`' :
                '"' . $value . '"';
        $this->customWhere('`:TBL:`.`' . Util::camelTo_($column) . '` <= ' . $value,
                $logicalAnd ? 'AND' : 'OR');
        return $this;
    }

    /**
     * Select a column where it is greater than the given value
     * @param string $column
     * @param mixed $value
     * @param boolean $logicalAnd Indicates whether to use logical AND (TRUE) or OR (FALSE)
     * @param boolean $valueIsColumn Indicates whether the value is another columnName
     * @return Table
     */
    public function greaterThan($column, $value, $logicalAnd = true,
            $valueIsColumn = false) {
        $value = $valueIsColumn ? '`:TBL:`.`' . \Util::camelTo_($value) . '`' :
                '"' . $value . '"';
        $this->customWhere('`:TBL:`.`' . Util::camelTo_($column) . '` > ' . $value,
                $logicalAnd ? 'AND' : 'OR');
        return $this;
    }

    /**
     * Select a column where it is greater than or equal to the given value
     * @param string $column
     * @param mixed $value
     * @param boolean $logicalAnd Indicates whether to use logical AND (TRUE) or OR (FALSE)
     * @param boolean $valueIsColumn Indicates whether the value is another columnName
     * @return Table
     */
    public function greaterThanOrEqualTo($column, $value, $logicalAnd = true,
            $valueIsColumn = false) {
        $value = $valueIsColumn ? '`:TBL:`.`' . \Util::camelTo_($value) . '`' :
                '"' . $value . '"';
        $this->customWhere('`:TBL:`.`' . Util::camelTo_($column) . '` >= ' . $value,
                $logicalAnd ? 'AND' : 'OR');
        return $this;
    }

    /**
     * Finds rows that matches the given range of values in the required column
     * @param string $column
     * @param mixed $value1
     * @param mixed $value2
     * @param boolean $logicalAnd Indicates whether to use the logical AND [TRUE] or OR [FALSE]
     */
    public function between($column, $value1, $value2, $logicalAnd = true) {
        $this->customWhere('`:TBL:`.`' . Util::camelTo_($column) . '` BETWEEN "'
                . $value1 . '" AND "' . $value2 . '" ',
                $logicalAnd ? 'AND' : 'OR');
    }

    /**
     * Query the table where the given values are equal to the corresponding
     * column value in the table
     * @param array $criteria
     * @param boolean $joinWithAnd Indicates whether to use logical AND (TRUE) or OR (FALSE)
     * @param boolean $valuesAreColumns Indicates whether the values are columnNames
     * @return Table
     */
    public function equal(array $criteria, $joinWithAnd = true,
            $valuesAreColumns = false) {
        $this->where($criteria, $joinWithAnd, false, $valuesAreColumns);
        return $this;
    }

    /**
     * Query the table where the given values are not equal to the corresponding
     * column value in the table
     * @param array $criteria
     * @param boolean $joinWithAnd Indicates whether to use logical AND (TRUE) or OR (FALSE)
     * @param boolean $valuesAreColumns Indicates whether the values are columnNames
     * @return Table
     */
    public function notEqual(array $criteria, $joinWithAnd = true,
            $valuesAreColumns = false) {
        $this->where($criteria, $joinWithAnd, true, $valuesAreColumns);
        return $this;
    }

    /**
     * Select a column where the value matches the regular expression
     * @param string $column
     * @param mixed $value
     * @param boolean $logicalAnd Indicates whether to use logical AND (TRUE) or OR (FALSE)
     * @return Table
     */
    public function regExp($column, $value, $logicalAnd = true) {
        $this->customWhere('`:TBL:`.`' . Util::camelTo_($column) . '` REGEXP "' . $value . '"',
                $logicalAnd ? 'AND' : 'OR');
        return $this;
    }

    /**
     * Select a column where the value does not match the regular expression
     * @param string $column
     * @param mixed $value
     * @param boolean $logicalAnd Indicates whether to use logical AND (TRUE) or OR (FALSE)
     * @return Table
     */
    public function notRegExp($column, $value, $logicalAnd = true) {
        $this->customWhere('`:TBL:`.`' . Util::camelTo_($column) . '` NOT REGEXP "' . $value . '"',
                $logicalAnd ? 'AND' : 'OR');
        return $this;
    }

    /**
     * Select a column where the value is null
     * @param string $column
     * @param boolean $logicalAnd Indicates whether to use logical AND (TRUE) or OR (FALSE)
     * @return Table
     */
    public function isNull($column, $logicalAnd = true) {
        $this->customWhere('`:TBL:`.`' . Util::camelTo_($column) . '` IS NULL',
                $logicalAnd ? 'AND' : 'OR');
        return $this;
    }

    /**
     * Select a column where the value is not null
     * @param string $column
     * @param boolean $logicalAnd Indicates whether to use logical AND (TRUE) or OR (FALSE)
     * @return Table
     */
    public function isNotNull($column, $logicalAnd = true) {
        $this->customWhere('`:TBL:`.`' . Util::camelTo_($column) . '` IS NOT NULL',
                $logicalAnd ? 'AND' : 'OR');
        return $this;
    }

    /**
     * Select a column where the values of the given columns are null
     * @param string $columns
     * @param boolean $logicalAnd Indicates whether to use logical AND (TRUE) or OR (FALSE)
     * @return Table
     */
    public function areNull(array $columns, $logicalAnd = true) {
        foreach ($columns as $column) {
            $this->isNull($column, $logicalAnd);
        }
        return $this;
    }

    /**
     * Select a column where the values of the given columns are not null
     * @param string $columns
     * @param boolean $logicalAnd Indicates whether to use logical AND (TRUE) or OR (FALSE)
     * @return Table
     */
    public function areNotNull(array $columns, $logicalAnd = true) {
        foreach ($columns as $column) {
            $this->isNotNull($column, $logicalAnd);
        }
        return $this;
    }

    /**
     * Start grouping criteria with a parenthesis
     * @param boolean $logicalAnd
     * @return \DBScribe\Table
     */
    public function startGroup($logicalAnd = true) {
        if ($this->where)
                $this->where .= ' ' . ($logicalAnd ? 'AND' : 'OR') . ' (';
        else $this->where = ' WHERE (';

        return $this;
    }

    /**
     * Ends the parenthesis group
     * @return \DBScribe\Table
     */
    public function endGroup() {
        if ($this->where) $this->where .= ')';
        return $this;
    }

    /**
     * Adds a custom query to the existing query. If no query exists, it serves as
     * the query.
     * @param string $custom
     * @param string $logicalConnector Logical operator to link the <i><b>custom where</b></i>
     * with the <i><b>regular where</b></i> if available
     * @param string $tablePlaceholder A string within the custom where to be
     * replaced with the table name. Useful when a table prefix might have been
     * used
     * @return Table
     */
    public function customWhere($custom, $logicalConnector = 'AND',
            $tablePlaceholder = ':TBL:') {
        if ($this->where && substr($this->where, strlen($this->where) - 1) != '(')
                $this->where .= ' ' . $logicalConnector;
        else if (!$this->where) $this->where = ' WHERE ';

        $this->where .= trim(str_replace($tablePlaceholder, $this->name, $custom));
        return $this;
    }

    /**
     * Group result by data in given column
     * @param string $columnName
     * @return Table
     */
    public function groupBy($columnName) {
        $this->groups[] = $columnName;
        return $this;
    }

    /**
     * Fetch rows that fulfill the given condition
     * @param string $condition Ready-made query e.g `:TBL:`.`id` > 2
     * @return Table
     */
    public function having($condition, $tablePlaceholder = ':TBL:') {
        $this->having = trim(str_replace($tablePlaceholder, $this->name,
                        $condition));
        return $this;
    }

    /**
     * Fetch results whose data in the given column is in the given array
     * of values
     * @param string $column
     * @param array $values
     * @param boolean $logicalAnd Indicates whether to join the in query to the
     * rest of the query with an AND (TRUE) or an OR (FALSE)
     * @param boolean $valuesAreColumns Indicates whether the values are columnNames
     * @return Table
     */
    public function in($column, array $values, $logicalAnd = true,
            $valuesAreColumns = false) {
        $this->where(array(array($column => $values)), $logicalAnd, false,
                $valuesAreColumns);
        return $this;
    }

    /**
     * Fetch results whose data in the given column are in the given array
     * of values
     * @param string $column
     * @param array $values
     * @param boolean $logicalAnd Indicates whether to join the in query to the
     * rest of the query with an AND (TRUE) or an OR (FALSE)
     * @param boolean $valuesAreColumns Indicates whether the values are columnNames
     * @return Table
     */
    public function notIn($column, array $values, $logicalAnd = true,
            $valuesAreColumns = false) {
        $this->where(array(array($column => $values)), $logicalAnd, true,
                $valuesAreColumns);
        return $this;
    }

    /**
     * Joins with the given table
     * @param string|Table $table
     * @param array $options Keys include [rowModel]
     */
    public function join($table, array $options = array()) {
        $this->joins[$table] = $options;
        return $this;
    }

    private function processJoins() {
        $this->joinQuery = '';
        $superStart = false;
        foreach ($this->joins as $table => $options) {
            $relationships = $this->getTableRelationships($table);
            if (!count($relationships)) continue;

            if (!is_object($table)) {
                $table = new Table($table, $this->connection);
            }

            $alias = substr($table->getName(), 0, 1) .
                    substr($table->getName(), count($table->getName()) - 1, 1);
            $this->query .= ', ' .
                    $this->prepareColumns($table,
                            ($table->getName() == $this->name) ?
                                    $alias : null);

            $this->joinQuery .= ' LEFT OUTER JOIN `' . $table->getName() . '`' .
                    (($table->getName() == $this->name) ? ' ' . $alias : null);

            $started = false;
            foreach ($relationships as $ky => $rel) {
                if (($rel['push'] && isset($options['pull']) && @$options['push']) ||
                        (!$rel['push'] && isset($options['pull']) && !$options['pull']))
                        continue;

                if ($ky && $started) $this->joinQuery .= 'OR ';
                if (!$started) $this->joinQuery .= ' ON ';
                $started = true;
                $superStart = true;
                $this->joinQuery .= '`' . $this->name . '`.`' . $rel['column'] .
                        '` = ' . (($table->getName() == $this->name) ?
                                $alias : '`' . $table->getName() . '`') . '.`' .
                        $rel['refColumn'] . '` ';

                if (isset($options['where'])) {
                    foreach ($options['where'] as $column => $value) {
                        $this->joinQuery .= 'AND `' . $table->getName() . '`.`' . Util::camelTo_($column) . '` = ? ';
                        $this->values[] = $value;
                    }
                }
            }
        }

        if ($this->joinQuery && !$superStart)
                throw new Exception('Joined table(s) must have something in common with the current table "' . $this->name . '"');

        $this->joins = array();

        return $this->joinQuery;
    }

    /**
     * Checks the joined data for rows that have the value needed in a column
     * @param string $tableName
     * @param array $columns Key to value of column to value
     * @param Row $object
     * @param array $options
     * @return ArrayCollection
     */
    final public function seekJoin($tableName, array $columns,
            Row $object = null, array $options = array()) {
        if (!$this->joinQuery) return false;

        $prefix = $this->connection->getTablePrefix() . $tableName . '_';

        if (!$object) {
            $object = new Row();
        }

        $array = array();
        foreach ($this->relationshipData as $data) {
            foreach ($columns as $column => $value) {
                $compare = $prefix . $column;
                if ($data[$compare] === null) continue;
                $found = true;
                if ((!is_array($value) && @$data[$compare] != $value) || (is_array($value) &&
                        !in_array(@$data[$compare], $value))) {
                    $found = false;
                }
                if (!$found) break;
            }

            if ($found) {
                $d = array();
                foreach ($data as $col => $val) {
                    $d[str_replace($prefix, '', $col)] = $val;
                }

                $ob = clone $object;

                $array[] = $ob->populate($d);
            }
        }

        $this->parseWithOptions($array, $options);

        return count($array) ? new ArrayCollection($array) : false;
    }

    private function parseWithOptions(array &$array, array $options) {
        if (isset($options[0]['orderBy'])) {
            usort($array,
                    function($a, $b) use($options) {
                if (is_array($options[0]['orderBy'])) {
                    foreach ($options[0]['orderBy'] as $order) {
                        $comp = is_array($order) ?
                                $this->compareOrder($order['position'], $a, $b) :
                                $this->compareOrder($order, $a, $b);
                        if ($comp) {
                            return $comp;
                        }
                    }
                }
                else {
                    return $this->compareOrder($options[0]['orderBy'], $a, $b);
                }
            });
        }

        $array = array_values($array);

        if (isset($options[0]['limit'])) {
            if (!isset($options[0]['limit']['start'])) {
                $options[0]['limit']['start'] = 0;
            }
            if (!isset($options[0]['limit']['count'])) {
                $options[0]['limit']['count'] = count($array) - (int) $options['0']['limit']['start'];
            }

            $array = array_slice($array, $options[0]['limit']['start'],
                    $options[0]['limit']['count']);
        }

        return $array;
    }

    private function compareOrder($order, $a, $b) {
        $method = 'get' . ucfirst($order);
        if (method_exists($a, $method)) {
            $value1 = $a->$method();
            $value2 = $b->$method();
        }
        else {
            $value1 = $a->$order;
            $value2 = $b->$order;
        }

        return strcmp($value1, $value2);
    }

    /**
     * Checks if the row is a valid \DBScribe\Row row
     * @param array|object $row
     * @param boolean $preSave Indicates whether to call the presave function of the row
     * @throws Exception
     * @return array|boolean
     */
    private function checkModel($row, $preSave = false) {
        if (!is_array($row) && !is_object($row))
                throw new Exception('Each element of param $where must be an object of, or one that extends, "DBScribe\Row", or an array of [column => value]: ' . print_r($row,
                    true));

        if (empty($this->columns)) return array();

        if (is_array($row)) {
            return $row;
        }
        elseif (is_object($row) && get_class($row) === 'DBScribe\Row' || in_array('DBScribe\Row',
                        class_parents($row))) {
            if ($preSave) $row->preSave();

            $this->rowModelInUse = $row;
            return $row->toArray(($this->current !== self::OP_SELECT));
        }
    }

    /**
     * Orders the returned rows
     * @param string $column
     * @param string $direction One of \DBScribe\Table::ORDER_ASC or \DBScribe\Table::ORDER_DESC
     * @return Table
     */
    public function orderBy($column, $direction = Table::ORDER_ASC) {
        $this->orderBy[] = '`' . Util::camelTo_($column) . '` ' . $direction;
        return $this;
    }

    /**
     * Limits the number of rows to return
     * @param int $count No of rows to return
     * @param int $start Row no to start from
     * @return Table
     */
    public function limit($count, $start = 0) {
        $this->limit = 'LIMIT ' . $start . ', ' . $count;
        return $this;
    }

    /**
     * Counts the number of rows in the table based on a column
     * @param string $column The column to count
     * @return Int
     */
    public function count($column = '*', $criteria = array(),
            $return = Table::RETURN_DEFAULT) {
        $this->query = 'SELECT COUNT(' . Util::camelTo_($column) . ') as rows FROM `' . $this->name . '`';
        $this->where($criteria);

        $this->setExpectedResult($return, true);
        if ($ret = $this->execute()) {
            return ($ret) ? $ret[0]['rows'] : 0;
        }
        return 0;
    }

    /**
     * Gets the distinct values of a column
     * @param string $column
     * @param array $criteria Array with values \DBScribe\Row or array of [column => value]
     * @return ArrayCollection
     */
    public function distinct($column, array $criteria = array(),
            $return = Table::RETURN_MODEL) {
        $this->current = self::OP_SELECT;
        $this->setExpectedResult($return, true);
        $this->targetColumns($column);
        $this->query = 'SELECT DISTINCT `' . $this->name . '`.`' . Util::camelTo_($column) . '` as ' . Util::_toCamel($column) . ' FROM `' . $this->name . '` ' . $this->joinQuery;
        $this->where($criteria);

        return $this->execute();
    }

    /**
     * Updates the given row(s) in the table<br />
     * Many rows can be updated at once.
     * @param array $values Array with values \DBScribe\Row or array of [column => value]
     * @param string $whereColumn Column name to check. Default is the id column
     * @todo Allow multiple columns as criteria where
     * @return Table
     */
    public function update(array $values, $whereColumn = 'id') {
        if (!$this->checkExists()) return false;
        $this->current = self::OP_UPDATE;
        $this->query = 'UPDATE `' . $this->name . '` SET ';

        if (!is_array($whereColumn)) $whereColumn = array($whereColumn);

        foreach ($whereColumn as &$col) {
            $col = Util::camelTo_($col);
        }

        $nColumns = 0;
        $columns = array();
        foreach (array_values($values) as $ky => $row) {
            $rowArray = $this->checkModel($row, true);
            if ($ky == 0) $nColumns = array_keys($rowArray);

            if (count(array_keys($rowArray)) !== count($nColumns))
                    throw new Exception('All rows must have the same number of columns in table "' . $this->name .
                '". Set others as null');

            if (count($rowArray) === 0)
                    throw new Exception('You cannot insert an empty row into table "' . $this->name . '"');

            $cnt = 1;
            foreach ($rowArray as $column => &$value) {
                if (empty($value) && $value != 0) continue;

                if ($cnt > 1 && !in_array($column, $nColumns)) {
                    throw new Exception('All rows must have the same column names.');
                }
                $column = Util::camelTo_($column);

                if ($this->getPrimaryKey() == $column) {
                    if (in_array($this->getPrimaryKey(), $whereColumn)) {
                        $this->values[$ky][':' . $column] = $value;
                    }
                    continue;
                }

                $this->values[$ky][':' . $column] = $value;
                if (in_array($column, array_merge($columns, $whereColumn))) {
                    $cnt++;
                    continue;
                }

                $this->query .= '`' . $column . '` = :' . $column;

                if (count($rowArray) > $cnt) $this->query .= ', ';
                $columns[] = $column;

                $cnt++;
            }
            foreach ($whereColumn as $column) {
                $column = Util::camelTo_($column);
                $this->value[$ky][':' . $column] = $rowArray[$column];
            }
        }

        $this->query = (substr($this->query, strlen($this->query) - 2) === ', ')
                    ?
                substr($this->query, 0, strlen($this->query) - 2) : $this->query;

        $this->query .= ' WHERE ';
        foreach ($whereColumn as $key => $where) {
            $where = Util::camelTo_($where);

            if ($key) $this->query .= ' AND ';
            $this->query .= '`' . $where . '`=:' . $where;
        }
        $this->multiple = true;
        $this->doPost = self::OP_UPDATE;
        if ($this->delayExecute) {
            return $this;
        }
        return $this->execute();
    }

    /**
     * Updates rows that exist and creates those that don't
     * @param array $values
     * @param string|integer|array $whereColumn
     * @param string|bool $generateIds If string, indicates a YES and the primary column name.
     * If bool TRUE, it indicates YES and primay column name 'id'
     * If bool FALSE, it indicates NO.
     * @return boolean
     * @todo Refactor to accomodate large bulk of data
     */
    public function upsert(array $values, $whereColumn = 'id',
            $generateIds = true) {
        if (!is_array($whereColumn)) $whereColumn = array($whereColumn);

        if (!$this->checkExists()) return false;

        $this->current = self::OP_INSERT;
        $this->query = 'INSERT INTO `' . $this->name . '` (';
        $columns = array();
        $noOfColumns = 0;
        $update = '';
        foreach (array_values($values) as $ky => $row) {
            $rowArray = $this->checkModel($row, true);
            if ($ky === 0) $noOfColumns = count($rowArray);

            if (count($rowArray) === 0)
                    throw new Exception('You cannot insert an empty row into table "' . $this->name . '"');

            if ($generateIds) {
                $id = is_string($generateIds) ? $generateIds : 'id';
                if (!array_key_exists($id, $rowArray))
                        $rowArray[$id] = Util::createGUID();
            }

            if ($generateIds) {
                $id = is_string($generateIds) ? $generateIds : 'id';
                if (!array_key_exists($id, $rowArray))
                        $rowArray[$id] = Util::createGUID();
            }

            foreach ($rowArray as $column => &$value) {
                if (empty($value) && $value != 0) continue;

                $column = Util::camelTo_($column);

                if (!$ky && !in_array($column, $whereColumn)) {
                    if ($update) $update .= ', ';
                    $update .= '`' . $column . '`=VALUES(' . $column . ')';
                }

                if (!in_array($column, $columns)) $columns[] = $column;
                $this->values[$ky][':' . $column] = $value;
            }
        }

        $this->query .= '`' . join('`, `', $columns) . '`';
        $this->query .= ') VALUES (';
        $this->query .= ':' . join(', :', $columns);
        $this->query .= ') ON DUPLICATE KEY UPDATE ';
        $this->query .= $update;

        $this->multiple = true;
        $this->doPost = self::OP_INSERT;
        if ($this->delayExecute) {
            return $this;
        }

        return $this->execute();
    }

    /**
     * Deletes the given row(s) in the table<br />
     * Many rows can be deleted at once.
     * @param array $criteria Array with values \DBScribe\Row or values of [column => value]
     * @return Table
     */
    public function delete(array $criteria = array()) {
        if (!$this->checkExists()) return false;

        $this->current = self::OP_DELETE;
        $this->query = 'DELETE FROM `' . $this->name . '`';
        if (!empty($criteria)) $this->query .= ' WHERE ';
        foreach ($criteria as $ky => $row) {
            $rowArray = $this->checkModel($row, false);
            $cnt = 0;
            foreach ($rowArray as $column => $value) {
                if (!is_object($value) && $value === null) {
                    continue;
                }
                $column = Util::camelTo_($column);

                if ($cnt) $this->query .= ' AND ';

                $this->query .= '`' . $column . '` = ?';
                $this->values[] = $value;
                $cnt++;
            }

            if ($ky < (count($criteria) - 1)) $this->query .= ' OR ';
        }

        if ($this->delayExecute) {
            return $this;
        }
        return $this->execute();
    }

    /**
     * Indicates whether to delay database operation until method execute() is called
     * @param boolean $delay
     * @return Table
     */
    public function delayExecute($delay = true) {
        $this->delayExecute = $delay;
        return $this;
    }

    /**
     * Allows reuse of parameters
     * @param boolean $bool
     * @return \DBScribe\Table
     */
    public function reuseParams($bool = true) {
        $this->reuseParams = $bool;
        return $this;
    }

    /**
     * Executes the delayed database operation
     * @return mixed
     */
    public function execute() {
        if (!$this->checkExists()) {
            if ($this->current === self::OP_SELECT) {
                return new ArrayCollection();
            }

            return false;
        }
        $this->createQuery();

        $model = ($this->expected) ? $this->getRowModel() : null;
        if ($this->current === self::OP_SELECT && $this->isCached()) {
            $result = $this->getCached();
        }
        if (!$result) {
            if ($this->preserveQueries && $this->current !== self::OP_SELECT) // keep all queries except selects
                    $this->doPreserveQueries();
            $result = $this->connection->doPrepare($this->query, $this->values,
                    array(
                'multipleRows' => $this->multiple,
                'model' => $model
            ));

            if ($this->current === self::OP_SELECT) $this->saveCache($result);
            else $this->removeCache();
        }

        if ($this->current === self::OP_SELECT)
                $result = $this->returnSelect($result);
        $this->lastQuery = $this->query . '<pre><code>' . print_r($this->values,
                        true) . '</code></pre>';
        $this->resetQuery();
        return $result;
    }

    /**
     * Fetches the last query executed
     * @return string
     */
    public function getLastQuery() {
        return $this->lastQuery;
    }

    private function createQuery() {
        if ($this->genQry) return $this->query;

        if (!empty($this->customWhere)) {
            if ($this->where) {
                $this->where .= ' ' . $this->customWhereJoin . ' ' . $this->customWhere;
            }
            else {
                $this->where = ' WHERE ' . $this->customWhere;
            }
        }
        if ($this->current === self::OP_SELECT) {
            if ($this->groups) {
                $this->where .= ' GROUP BY ';
                foreach ($this->groups as $ky => $column) {
                    if ($ky) $this->where .= ', ';
                    $this->where .= '`' . $this->name . '`.`' . $column . '`';
                }
            }
            if ($this->having) {
                $this->where .= ' HAVING ' . $this->having;
            }
        }
        if (!empty($this->orderBy)) {
            $this->where .= ' ORDER BY ';
            foreach ($this->orderBy as $ky => $order) {
                $this->where .= $order;
                if ($ky < (count($this->orderBy) - 1)) $this->where .= ', ';
            }
        }

        $this->where .= ' ' . $this->limit;

        if (($this->current === self::OP_SELECT || !empty($this->customWhere)) &&
                !stristr($this->query, ' from')) {
            $this->query .= ' FROM `' . $this->name . '`';
        }
        $this->query .= $this->where;

        $this->genQry = true;
        return $this->query;
    }

    /**
     * Fetches the query to execute
     * @param bool $withValues
     * @return string
     */
    public function getQuery($withValues = false) {
        return $withValues ? $this->createQuery() . '<pre><code>' . print_r($this->getQueryValues(),
                        true) . '</code></pre>' : $this->createQuery();
    }

    /**
     * Fetches the values to pass in to the query
     * @return array
     */
    public function getQueryValues() {
        return $this->values;
    }

    private function resetQuery() {
        $this->query = null;
        $this->genQry = false;
        $this->targetColumns = null;
        $this->orderBy = array();
        $this->limit = null;
        $this->customWhere = null;
        if (!$this->reuseParams) {
            $this->where = null;
            $this->values = null;
        }
        $this->having = null;
        $this->groups = array();
        $this->current = null;
        $this->multiple = false;
        $this->expected = null;
    }

    /**
     * Fetches the autogenerated id of the last insert statement, if primary key is autogenerated
     * @return mixed
     */
    public function lastInsertId() {
        return $this->connection->lastInsertId();
    }

}
