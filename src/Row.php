<?php

namespace dbScribe;

/**
 * This is a typical row in the database table where the properties are the
 * columns.
 *
 * @author Ezra Obiwale <contact@ezraobiwale.com>
 */
class Row extends Commons implements \JsonSerializable {

	/**
	 * The connection object
	 * @var Connection
	 */
	private $__connection;

	/**
	 * Array of row values as extracted from the database
	 * @var array
	 */
	private $__content;

	/**
	 * Array of joined table values
	 * @var array
	 */
	private $__joined;

	/**
	 *
	 * @var \dbScribe\Table
	 */
	private $__table;

	/**
	 * Name of the table to attach model to
	 * @var string|null
	 */
	private $__tableName;

	/**
	 * Sets the name of the table to attach model to
	 * @param string $tableName
	 * @return \dbScribe\Row
	 */
	final public function setTableName($tableName) {
		$this->__tableName = $tableName;
		return $this;
	}

	/**
	 * Fetches the name of the table to attach model to
	 * @return string
	 */
	final public function getTableName() {
		if ($this->__tableName !== null) return $this->__tableName;

		$exp = explode('\\', get_called_class());
		return \Util::camelTo_($exp[count($exp) - 1]);
	}

	/**
	 * Populates the properties of the model from the given data
	 * @param array $data
	 * @return \dbScribe\Row
	 */
	final public function populate(array $data) {
		foreach ($data as $property => $value) {
			$property = \Util::_toCamel($property);
			$method = 'set' . ucfirst($property);
			if (method_exists($this, $method)) {
				$this->{$method}($value);
			}
			else {
				$this->{$property} = $value;
			}
		}

		return $this;
	}

	final public function __setup(array $data) {
		$joined = array();
		foreach ($data as $name => $value) {
			if (method_exists($this, 'set' . $name)) $this->{'set' . $name}($value);
			else if (stristr($name, Table::SEPARATOR)) { // is a joined value
				$split = explode(Table::SEPARATOR, $name);
				$_tableName = $split[0];
				unset($split[0]);
				// Join to allow further join breakdowns 
				$_colName = join(Table::SEPARATOR, $split);
				$joined[$_tableName][0][$_colName] = $value;
			}
			else $this->{$name} = $value;
		}
		if (count($joined)) $this->__setJoined($joined);
		return $this;
	}

	/**
	 * Sets the fetched joined values for the row
	 * @param array $joined
	 * @return \dbScribe\Row
	 */
	final public function __setJoined(array $joined) {
		$this->__joined = $joined;
		return $this;
	}

	/**
	 * Fetches the joined values for a given column
	 * @param string $column The column for which to get joined values
	 * @return array|null
	 */
	final protected function __getJoined($column = null) {
		return $column ? $this->__joined[$column] : $this->__joined;
	}

	/**
	 * Checks whether the given table data is joined
	 * @param string $tableName
	 * @return boolean
	 */
	final protected function isJoined($tableName) {
		return is_array($this->__joined) && array_key_exists($tableName, $this->__joined);
	}

	/**
	 * Sets the default row values as obtained from the database
	 * @param array $__content
	 * @return \dbScribe\Row
	 */
	final public function __setContent(array $__content) {
		$this->__content = $__content;
		return $this;
	}

	/**
	 * Checks if a property exists in the model
	 * @param string $propertyName
	 * @return boolean
	 */
	public function hasProperty($propertyName) {
		return property_exists($this, $propertyName);
	}

	/**
	 * Returns an array copy of the properties of the row and their values
	 * @return array
	 */
	public function toArray() {
		$ppts = get_object_vars($this);
		unset($ppts['__connection']);
		unset($ppts['__tableName']);
		unset($ppts['__by']);
		unset($ppts['__table']);
		unset($ppts['__content']);
		unset($ppts['__joined']);
		return $ppts;
	}

	/**
	 * Resets all properties of the model to null
	 * @param array $contructorParameters Array of parameters to pass to model
	 * constructor, if any.
	 * @return \dbScribe\Row
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
	 * @param \dbScribe\Connection $connection
	 * @return \dbScribe\Row
	 */
	final public function setConnection(Connection $connection) {
		$this->__connection = $connection;
		return $this;
	}

	/**
	 * Allows setting properties of the class publicly
	 * @param string $property
	 * @param mixed $value
	 * @throws \Exception
	 */
	final public function __set($property, $value) {
		if (in_array($property, array('__connection', '__content', '__tableName', '__table', '__joinings')))
				throw new \Exception('Property "' . $property . '" is reserved.');
		$this->{$property} = $value;
		$this->_set($property, $value);
	}

	/**
	 * Replaces magic method __set() for children classes
	 * @param string $property
	 * @param mixed $value
	 * @return \dbScribe\Row
	 */
	protected function _set($property, $value) {
		
	}

	/**
	 * Allow calling related tables
	 * @param string $name
	 * @param array $args Array of options::
	 *      backward - Fetch rows where the current table is being referenced
	 *      forward - Fetch rows referenced by current table
	 * 		where - array of array of columns => values criteria
	 *      model- Model to parse the returned rows into
	 *      limit- Limit the number of rows to fetch @see Table::limit()
	 *      orderBy- Sort the fetched rows @see Table::orderBy()
	 * @return null
	 */
	final public function __call($name, $args) {
		// magic set and get methods
		$property = substr($name, 3);
		if (substr(strtolower($name), 0, 3) === 'set') {
			if (!property_exists($this, $property)) $property = lcfirst($property);
			if (!property_exists($this, $property)) $property = Util::_toCamel($property);
			if (!property_exists($this, $property)) $property = Util::camelTo_($property);
			if (property_exists($this, $property)) {
				$this->{$property} = $args[0];
				return $this;
			}
		}
		else if (substr(strtolower($name), 0, 3) === 'get') {
			if (!property_exists($this, $property)) $property = lcfirst($property);
			if (!property_exists($this, $property)) $property = Util::_toCamel($property);
			if (!property_exists($this, $property)) $property = Util::camelTo_($property);
			if (property_exists($this, $property)) return $this->{$property};
		}
		unset($property);

		// handle referencing

		if ($this->getTable() && !$this->__connection) {
			$this->setConnection($this->__table->getConnection());
		}
		else if (!$this->__connection && !$this->__table) {
			return new ArrayCollection;
		}

		// Check if already eager loaded
		$result = $this->__getJoined($name);
		if (NULL !== ($ret = $this->_preCall($name, $args, $result))) {
			return $ret;
		}
		if ($result) {
			// parse result into the expected type
			$result = $this->__table
					->parseSelectResult($result, $args[0]['returnType'], $args[0]['rowModel']);
		}
		// select from required table since not eager joined
		else {
			$relTable = null;
			// Not a column, therefore $name is table
			if (!in_array($name, $this->__table->getColumns(true)) &&
					$relationships = $this->__table->getTableRelationships($name)) {
				foreach ($relationships as $relationship) {
					$column = Util::_toCamel($relationship['column']);
					if (isset($args[0]['backward']) && $args[0]['backward'] &&
							!$relationship['backward'] || isset($args[0]['forward']) &&
							$args[0]['forward'] && $relationship['backward']) continue;
					if (property_exists($this, $column)) {
						$args[0]['where'] = $this->getRelTableWhere($args, $relationship['refColumn'],
												  $this->{$column});
						break;
					}
				}
				$relTable = $this->__connection->table(Util::camelTo_($name));
			}
			else {
				if (!is_object($args[0]['rowModel'])) $relTable = $this->__connection->table($name);
				else $relTable = $args[0]['rowModel']->getTable();
			}

			if (!count($args[0]['where']) && !count($args[0]['in']))
			// Ensure that not all rows are returned
				return new ArrayCollection;
			$relTable->setRowModel($this->getRelTableModel($args))
					->setExpectedResult(isset($args[0]['returnType']) ?
									$args[0]['returnType'] : Table::RETURN_MODEL);
			$result = $this->prepSelectRelTable($relTable, $args);
			if (is_object($result) && is_a($result, get_class($relTable))) {
				$result = $result->select();
			}
			if (is_bool($result)) {
				switch ($args[0]['returnType']) {
					case Table::RETURN_DEFAULT: return array();
					case Table::RETURN_MODEL: return new ArrayCollection();
					case Table::RETURN_JSON: return json_encode(array());
				}
			}
			else if (is_a($result, get_class($relTable))) $result = $result->execute();
			$this->__setJoined(array(
				$name => $relTable->getRawResult(),
			));
		}


		return $result;
	}

	/**
	 * Called before using the magic method __call()
	 * @param type $property
	 * @param array $args
	 * @param array|null $joined Found joined values
	 */
	protected function _preCall($name, array & $args, $joined = null) {
		if (!method_exists($this, $name)) {
			if (!property_exists($this, $name)) {
				$property = lcfirst($property);
				if (!property_exists($this, $property)) $property = Util::_toCamel($property);
				if (!property_exists($this, $property)) $property = Util::camelTo_($property);
			}
			else $property = $name;
			// join

			if (!$joined && (($property && $this->{$property}) || $this->{$name})) {
				// $name is column and exists
				if ($ref = $this->__table->getReferences($name)) {
					$column = $ref['refColumn'];
					if (array_key_exists('where', $args[0])) {
						foreach ($args[0]['where'] as &$array) {
							$array[$column] = is_array($this->$property) ?
									$this->{$property}[0] : $this->$property;
						}
					}
					else {
						$args[0]['where'][] = array($column => is_array($this->$property) ?
									$this->{$property}[0] : $this->$property);
					}
				}
			}
			// reset set model
			if (isset($args[0]['model'])) {
				$args[0]['rowModel'] = $args[0]['model'];
				unset($args[0]['model']);
			}
		}
	}

	/**
	 * Prepares the related table for select, calling limit and orderBy if required
	 * @param \dbScribe\Table $relTable
	 * @param array $callArgs
	 * @return \dbScribe\Table
	 */
	private function prepSelectRelTable(Table & $relTable, array $callArgs) {
		foreach ($callArgs[0] as $method => $args) {
			if (!method_exists($relTable, $method)) $method = 'set' . ucfirst($method);
			if (method_exists($relTable, $method)) {
				$args = (is_array($args) && $method !== 'where') ? $args : array($args);
				$relTable = call_user_func_array(array($relTable, $method), $args);
			}
		}
		return $relTable;
	}

	/**
	 * Fetches the model to use for the called related table from the call arguments
	 * @param array $callArgs
	 * @return \dbScribe\Row | null
	 */
	private function getRelTableModel(array $callArgs) {
		if (isset($callArgs[0]['rowModel']) && is_a($callArgs[0]['model'], get_class())) {
			return $callArgs[0]['rowModel'];
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
	private function getRelTableWhere(array & $callArgs, $column, $value) {
		$where = array();

		if (!isset($callArgs [0]) || (isset($callArgs [0]) && !is_array($callArgs[0])) || (isset($callArgs [0]) &&
				is_array($callArgs [0]) && !isset($callArgs[0]['where']))) {
			$where[] = array($column => $value);
		}
		else if ((isset($callArgs [0]) && is_array($callArgs [0]) && isset($callArgs[0]['where']))) {
			if (is_array($callArgs[0]['where'])) {
				foreach ($callArgs[0]['where'] as $row) {
					if (is_object($row)) {
						$row->$column = $value;
					}
					else if (is_array($row)) {
						$row[$column] = $value;
					}
					else {
						throw new \Exception('Related Table Call Error: Option where value must be an array of \dbScribe\Rows or array of columns to values');
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
	 * Fetches the connection object
	 * @return \dbScribe\Connection|null
	 */
	final public function getConnection() {
		return $this->__connection;
	}

	/**
	 * Function to call before saving the model
	 */
	public function preSave() {
		
	}

	/**
	 * This is called after inserts and updates and expects a return of the model id.
	 * @param mixed $lastInsertId
	 * @param int $lastInsertId either dbScribe\Table::OP_INSERT or dbScribe\Table::OP_UPDATE
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
		$this->__setContent($this->toArray());
	}

	/**
	 * Fetches the vale of a property/column as it is in the database
	 * @param string $property
	 * @return mixed
	 * @deprecated
	 */
	public function getDBValue($property) {
		return $this->getOldValue($property);
	}

	/**
	 * Fetchs the value of a property/column as it iwere before a change
	 * @param string $property
	 * @return mixed
	 */
	final public function getOldValue($property) {
		return array_key_exists($property, $this->__content) ?
				$this->__content[$property] : $this->{$property
				}

		;
	}

	final public function setTable(Table $table) {
		$this->__table = $table;
		return $this;
	}

	/**
	 * Fetches the table in which the row exists
	 * @return Table|NULL
	 */
	public function getTable() {
		if (!$this->__table && $this->getConnection())
				$this->__table = $this->getConnection()->table($this->getTableName());
		return $this->__table;
	}

	/**
	 * Allows for serializing to json when passed into json_encode
	 * @return array
	 */
	public function jsonSerialize() {
		return $this->toArray();
	}

	/**
	 * Encodes the properties into a json string
	 * @return string
	 */
	public function toJson() {
		return json_encode($this);
	}

	/**
	 * Checks if the values of the properties have been changed
	 * @param string $property Property to check if has changed
	 * @return bool|array
	 */
	public function hasChanged($property = null) {
		if (!$this->__content) return false;
		if ($property) {
			$old = $this->__content[$property];
			$new = $this->{$property};
			if ((is_array($old) && !is_array($new))) return $old;
			else if (is_array($new) && !is_array($old)) return true;
			else if (is_array($old)) {
				return $this->arrayHasChanged($property, $old);
			}
			else return ($old == $new);
		}
		foreach ($this->__content as $prop => $val) {
			if (is_array($val)) {
				if ($this->arrayHasChanged($prop, $val)) {
					return true;
				}
			}
			else {
				if ($val != $this->{$prop}) return true;
			}
		}
		return false;
	}

	private function arrayHasChanged($property, array $value) {
		if (!is_array($this->{$property })) return true;
		return Util::arrayDiff($this->{ $property}, $value);
	}

}
