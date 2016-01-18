<?php

namespace DBScribe;

/**
 * This is a typical row in the database table where the properties are the
 * columns.
 *
 * @author Ezra Obiwale <contact@ezraobiwale.com>
 */
class Row implements \JsonSerializable {

    private $_connection;
    private $_by;
    private $_content;

    /**
     *
     * @var \DBScribe\Table
     */
    protected $_table;

    /**
     * Name of the table to attach model to
     * @var string|null
     */
    private $_tableName;

    /**
     * Sets the name of the table to attach model to
     * @param string $tableName
     * @return \DBScribe\Row
     */
    final public function setTableName($tableName) {
        $this->_tableName = $tableName;
        return $this;
    }

    /**
     * Fetches the name of the table to attach model to
     * @return string
     */
    final public function getTableName() {
        if ($this->_tableName !== null) return $this->_tableName;

        $exp = explode('\\', get_called_class());
        return \Util::camelTo_($exp[count($exp) - 1]);
    }

    /**
     * Populates the properties of the model from the given data
     * @param array $data
     * @return \DBScribe\Row
     */
    public function populate(array $data) {
        foreach ($data as $property => $value) {
            $property = \Util::_toCamel($property);
            $method = 'set' . ucfirst($property);
            if (method_exists($this, $method)) {
                $this->$method($value);
            }
            elseif (property_exists($this, $property)) {
                $this->$property = $value;
            }
        }

        return $this;
    }

    /**
     * Returns an array copy of the properties of the row and their values
     * @return array
     */
    public function toArray() {
        $ppts = get_object_vars($this);
        unset($ppts['_connection']);
        unset($ppts['_tableName']);
        unset($ppts['_by']);
        unset($ppts['_table']);
        unset($ppts['_content']);
        return $ppts;
    }

    /**
     * Resets all properties of the model to null
     * @param array $contructorParameters Array of parameters to pass to model
     * constructor, if any.
     * @return \DBScribe\Row
     */
    final public function reset(array $contructorParameters = array()) {
        foreach (array_keys($this->toArray()) as $ppt) {
            $this->$ppt = null;
        }

        if (method_exists($this, '__construct')) {
            call_user_func_array(array($this, '__construct'),
                    $contructorParameters);
        }

        return $this;
    }

    /**
     * Sets the connection for the use within row
     * @param \DBScribe\Connection $connection
     * @return \DBScribe\Row
     */
    final public function setConnection(Connection $connection) {
        $this->_connection = $connection;
        return $this;
    }

    /**
     * Allows setting properties of the class publicly
     * @param string $property
     * @param mixed $value
     * @throws \Exception
     */
    final public function __set($property, $value) {
        if (in_array($property, array('_connection', '_by', '_content', '_tableName', '_table')))
                throw new \Exception('Property "' . $property . '" is reserved.');
        $this->_set($property, $value);
    }

    /**
     * Replaces magic method __set() for children classes
     * @param string $property
     * @param mixed $value
     * @return \DBScribe\Row
     */
    protected function _set($property, $value) {
        $this->{$property} = $value;

        return $this;
    }

    /**
     * Allow calling related tables
     * @param string $name
     * @param array $args Array of options::
     *      push - Fetch rows where the current table is being referenced [PUSH ONTO CALLERS]
     *      pull - Fetch rows referenced by current table [PULL INTO CURRENT]
     *      model- Model to parse the returned rows into
     *      limit- Limit the number of rows to fetch @see Table::limit()
     *      orderBy- Sort the fetched rows @see Table::orderBy()
     * @return null
     */
    final public function __call($name, $args) {
        if (!method_exists($this, $name)) {
            if (!$this->_connection && $this->_table) {
                $this->_connection = $this->_table->getConnection();
            }
            else if (!$this->_table && $this->_connection) {
                $this->_table = $this->_connection->table($this->getTableName(),
                        $this);
            }
            else if (!$this->_connection && !$this->_table) {
                return new ArrayCollection;
            }
        }
        if (NULL !== ($return = $this->_preCall($name, $args))) {
            return $return;
        }
        if (!method_exists($this, $name)) {
            $where = array();
            $_name = $this->_connection->getTablePrefix() . Util::camelTo_($name);
            if (substr($name, 0, 2) == 'by') {
                $relTable = $this->_connection->table($name);
                $relTable->setRowModel($this->getRelTableModel($args));
                if (!$relTable->exists()) {
                    $this->_by = lcfirst(substr($name, 2));
                }
                return $this;
            }
            if (is_array($args['relateWhere']) && count($args['relateWhere'])) {
                $where = $args['relateWhere'];
            }
            else if ($relationships = $this->_table->getTableRelationships($name)) {
                $by = Util::camelTo_($this->_by);
                foreach ($relationships as $relationship) {
                    if ($by && $relationship['column'] !== $by) {
                        continue;
                    }

                    $column = Util::_toCamel($relationship['column']);
                    if (isset($args[0]['push']) && $args[0]['push'] && !$relationship['push'] ||
                            isset($args[0]['pull']) && $args[0]['pull'] && $relationship['push'])
                            continue;
                    if (property_exists($this, $column)) {
                        $where = array_merge($where,
                                $this->getRelTableWhere($args,
                                        $relationship['refColumn'],
                                        $this->{$column}));
                    }
                }
            }
            $this->_by = null;
            if (!count($where)) // Ensure that not all rows are returned
                    return new ArrayCollection;
            // check joined tables' results
            $compressed = Util::compressArray($where);
            $return = $this->_table->seekJoin($name, $compressed,
                    $this->getRelTableModel($args), $args);

            // select from required table if not joined
            if (!$return) {
                if (!isset($relTable)) {
                    $relTable = $this->_connection->table(Util::camelTo_($name));
                    $relTable->setRowModel($this->getRelTableModel($args));
                }
                $relTable->where($where)
                        ->setExpectedResult(isset($args[0]['returnType']) ?
                                        $args[0]['returnType'] : Table::RETURN_MODEL);
                $return = $this->prepSelectRelTable($relTable, $args);
                if (is_object($return) && is_a($return, 'DBScribe\Table')) {
                    $return = $return->select();
                }
                if (is_bool($return)) {
                    return new ArrayCollection;
                }
            }
            return $return;
        }
    }

    /**
     * Called before using the magic method __call()
     * @param type $name
     * @param array $args
     */
    protected function _preCall(&$name, array &$args) {

    }

    /**
     * Prepares the related table for select, calling limit and orderBy if required
     * @param \DBScribe\Table $relTable
     * @param array $callArgs
     * @return \DBScribe\Table
     */
    private function prepSelectRelTable(Table &$relTable, array $callArgs) {
        foreach ($callArgs[0] as $method => $args) {
            if ($method === 'where') continue;
            if (method_exists($relTable, $method)) {
                $args = is_array($args) ? $args : array($args);
                $relTable = call_user_func_array(array($relTable, $method),
                        $args);
            }
        }
        return $relTable;
    }

    /**
     * Fetches the model to use for the called related table from the call arguments
     * @param array $callArgs
     * @return \DBScribe\Row | null
     */
    private function getRelTableModel(array $callArgs) {
        if (isset($callArgs['model']) && is_object($callArgs['model'])) {
            return $callArgs['model'];
        }
        else if (isset($callArgs['model']) && is_array($callArgs['model']) && isset($callArgs['model']['rowModel'])) {
            return $callArgs['model']['rowModel'];
        }

        return new Row();
    }

    /**
     * Fetches the where values for the related table from the call arguments
     *
     * @param array $callArgs
     * @param string $column
     * @param mixed $value
     * @return array
     * @throws \Exception
     */
    private function getRelTableWhere(array &$callArgs, $column, $value) {
        $where = array();

        if (!isset($callArgs[0]) || (isset($callArgs[0]) && !is_array($callArgs[0])) ||
                (isset($callArgs[0]) && is_array($callArgs[0]) && !isset($callArgs[0]['where']))) {
            $where[] = array($column => $value);
        }
        else if ((isset($callArgs[0]) && is_array($callArgs[0]) && isset($callArgs[0]['where']))) {
            if (is_array($callArgs[0]['where'])) {
                foreach ($callArgs[0]['where'] as $row) {
                    if (is_object($row)) {
                        $row->$column = $value;
                    }
                    else if (is_array($row)) {
                        $row[$column] = $value;
                    }
                    else {
                        throw new \Exception('Related Table Call Error: Option where value must be an array of \DBScribe\Rows or array of columns to values');
                    }

                    $where[] = $row;
                }
            }
            else if (is_object($row)) {
                $row->$column = $value;
                $where[] = $row;
            }
            unset($callArgs[0]['where']);
        }

        return $where;
    }

    /**
     * Fetches the connection object
     * @return \DBScribe\Connection|null
     */
    final public function getConnection() {
        return $this->_connection;
    }

    /**
     * Function to call before saving the model
     */
    public function preSave() {

    }

    /**
     * This is called after inserts and updates and expects a return of the model id.
     * @param mixed $lastInsertId
     * @param int $lastInsertId either DBScribe\Table::OP_INSERT or DBScribe\Table::OP_UPDATE
     * @param mixed $result Result of the operation
     * @return mixed Id of the model
     */
    public function postSave($operation, $result, $lastInsertId) {
        return $lastInsertId;
    }

    /**
     * Function to call after fetching from the db
     * @param string $property Property to act on
     */
    public function postFetch($property = null) {
        $this->_content = get_object_vars($this);
    }

    /**
     * Fetchs the value of a property/column as it is in the database
     * @param string $property
     * @return mixed
     */
    final public function getDBValue($property) {
        return $this->_content[$property];
    }

    final public function setTable(Table $table) {
        $this->_table = $table;
        return $this;
    }

    /**
     * Fetches the table in which the row exists
     * @return Table|NULL
     */
    public function getTable() {
        return $this->_table;
    }

    public function jsonSerialize() {
        return json_encode($this->toArray());
    }

}
