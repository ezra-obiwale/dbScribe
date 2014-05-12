<?php

namespace DBScribe;

class Row implements \JsonSerializable {

    private $connection;
    private $relationships = array();
    private $by;
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
        if ($this->_tableName !== null)
            return $this->_tableName;

        $exp = explode('\\', get_called_class());
        return \Util::camelTo_($exp[count($exp) - 1]);
    }

    /**
     * Populates the properties of the model from the given data
     * @param array $data
     * @return \DBScribe\Row
     */
    final public function populate(array $data) {
        foreach ($data as $property => $value) {
            $method = 'set' . ucfirst($property);
            if (method_exists($this, $method)) {
                $this->$method($value);
            }
            else {
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
        unset($ppts['connection']);
        unset($ppts['relationships']);
        unset($ppts['_tableName']);
        unset($ppts['by']);
        unset($ppts['_table']);
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
            call_user_func_array(array($this, '__construct'), $contructorParameters);
        }

        return $this;
    }

    /**
     * Sets the connection for the use within row
     * @param \DBScribe\Connection $connection
     * @return \DBScribe\Row
     */
    final public function setConnection(Connection $connection) {
        $this->connection = $connection;
        return $this;
    }

    /**
     * Sets the relationships which row may reach out to
     * @param array $relationships
     * @return \DBScribe\Row
     */
    final public function setRelationships(array $relationships) {
        $this->relationships = $relationships;
        return $this;
    }

    /**
     * Fetches the relationship between this table and the given table
     * @param string $tableName Name of the table to get the relationship
     * @return array
     */
    public function getRelationship($tableName) {
        return @$this->relationships[$tableName];
    }

    /**
     * Allows setting properties of the class publicly
     * @param string $property
     * @param mixed $value
     * @throws \Exception
     */
    final public function __set($property, $value) {
        if (in_array($property, array('_connection', '_relationships')))
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
        if (NULL !== $return = $this->_call($name, $args))
            return $return;
        
        if (!method_exists($this, $name)) {
            $_name = Util::camelTo_($name);
            if (substr($name, 0, 2) == 'by') {
                if ($this->connection !== null) {
                    $relTable = call_user_func_array(array($this->connection, 'table'), array($_name, $this->getRelTableModel($args)));
                    if (!$relTable->exists()) {
                        $this->by = lcfirst(substr($name, 2));
                    }
                    return $this;
                }
            }

            $where = array();

            if (isset($this->relationships[$_name])) {
                $by = Util::camelTo_($this->by);
                foreach ($this->relationships[$_name] as $relationships) {
                    if ($by && $relationships['column'] !== $by) {
                        continue;
                    }

                    $column = Util::_toCamel($relationships['column']);
                    if (isset($args[0]['push']) && $args[0]['push'] && !$relationships['push'] ||
                            isset($args[0]['pull']) && $args[0]['pull'] && $relationships['push'])
                        continue;
                    if (property_exists($this, $column)) {
                        $where = $this->getRelTableWhere($args, $relationships['refColumn'], $this->{$column});
                        break;
                    }
                }
            }
            else if (isset($args['relateWhere']) && is_array($args['relateWhere'])) {
                $where = $args['relateWhere'];
            }

            $this->by = null;
            if (empty($where))
                return new ArrayCollection;
            
            // check joined tables' results
            if ($this->_table) {
                $compressed = Util::compressArray($where);
                $return = $this->_table->seekJoin($name, $compressed, $this->getRelTableModel($args), $args);
            }
            // select from required table if not joined
            if (!$return) {
                if ($this->connection === null)
                    return null;

                if (!isset($relTable))
                    $relTable = call_user_func_array(array($this->connection, 'table'), array($_name, $this->getRelTableModel($args)));

                $return = $this->prepSelectRelTable($relTable, $args)->select($where);
                if (is_bool($return)) {
                    return new ArrayCollection;
                }
            }

            return $return;
        }
    }

    /**
     * Prepares the related table for select, calling limit and orderBy if required
     * @param \DBScribe\Table $relTable
     * @param array $callArgs
     * @return \DBScribe\Table
     */
    private function prepSelectRelTable(Table &$relTable, array $callArgs) {
        if ((isset($callArgs[0]) && is_array($callArgs[0]))) {
            if (isset($callArgs[0]['limit']) && is_array($callArgs[0]['limit'])) {
                if (isset($callArgs[0]['limit']['count'])) {
                    if (isset($callArgs[0]['limit']['start'])) {
                        $relTable->limit($callArgs[0]['limit']['count'], $callArgs[0]['limit']['start']);
                    }
                    else {
                        $relTable->limit($callArgs[0]['limit']['count']);
                    }
                }
            }

            if (isset($callArgs[0]['orderBy'])) {
                if (is_array($callArgs[0]['orderBy'])) {
                    foreach ($callArgs[0]['orderBy'] as $orderBy) {
                        if (is_array($orderBy) && isset($orderBy['column'])) {
                            if (isset($orderBy['direction'])) {
                                $relTable->orderBy($orderBy['column'], $orderBy['direction']);
                            }
                            else {
                                $relTable->orderBy($orderBy['column']);
                            }
                        }
                        else if (!is_array($orderBy)) {
                            $relTable->orderBy($orderBy);
                        }
                    }
                }
                else {
                    $relTable->orderBy($callArgs[0]['orderBy']);
                }
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
    private function getRelTableWhere(array $callArgs, $column, $value) {
        $where = array();

        if (!isset($callArgs[0]) || (isset($callArgs[0]) && !is_array($callArgs[0])) || (isset($callArgs[0]) && is_array($callArgs[0]) && !isset($callArgs[0]['where']))) {
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
        }

        return $where;
    }

    /**
     * Replace magic method __call() for children classes
     * @param type $name
     * @param array $args
     */
    protected function _call(&$name, array &$args) {
        
    }

    /**
     * Fetches the connection object
     * @return \DBScribe\Connection|null
     */
    final protected function getConnection() {
        return $this->connection;
    }

    /**
     * Fetches the relationships of the row
     * @return array
     */
    final protected function getRelationships() {
        return $this->relationships;
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
     */
    public function postFetch() {
        
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
        return $this->toArray();
    }

}
