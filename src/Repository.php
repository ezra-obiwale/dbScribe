<?php

namespace DBScribe;

use Exception;

/**
 * This class provides additional methods to those in the table to make
 * operations easier. It should be used when using the Mapper class.
 *
 * @author Ezra Obiwale <contact@ezraobiwale.com>
 */
class Repository extends Table {

    private $isSelect;
    private $alwaysJoin;

    /**
     * Class constructor
     * @param Mapper $table
     * @param Connection $connection
     * @param bool $delayExecution
     */
    public function __construct(Mapper $table, Connection $connection = null,
            $delayExecution = false) {
        if ($connection) {
            $table->setConnection($connection);
        }
        else {
            $connection = $table->getConnection();
        }
        if (!$connection)
                throw new Exception('Repository must have a valid connection: ' . $table->getTableName());

        parent::__construct($table->getTableName(), $connection, $table);
        $table->init($this);
        if ($delayExecution) $this->delayExecute();
        $this->alwaysJoin = array();
    }

    /**
     * Fetches all rows in the database
     * @param int $returnType Indicates the type of return expected. 
     * Possible values (Table::RETURN_MODEL | Table::RETURN_JSON | Table::RETURN_DEFAULT)
     * @return mixed
     */
    public function fetchAll($returnType = Table::RETURN_MODEL) {
        $return = $this->select(array(), $returnType);
        if (is_a($return, 'DBScribe\Table')) $return = $return->execute();
        if (is_bool($return)) return new ArrayCollection();

        return $return;
    }

    /**
     * Finds row(s) by a column value
     * @param string $column
     * @param mixed $value
     * @param int $returnType Indicates the type of return expected. 
     * Possible values (Table::RETURN_MODEL | Table::RETURN_JSON | Table::RETURN_DEFAULT)
     * @return mixed
     */
    public function findBy($column, $value, $returnType = Table::RETURN_MODEL) {
        $return = $this->select(array(array(Util::camelTo_($column) => $value)),
                $returnType);
        if (is_a($return, 'DBScribe\Table')) $return = $return->execute();
        if (is_bool($return)) return new ArrayCollection();

        return $return;
    }

    /**
     * Finds a row by a column value
     * @param column $column
     * @param mixed $value
     * @param int $returnType Indicates the type of return expected. 
     * Possible values (Table::RETURN_MODEL | Table::RETURN_JSON | Table::RETURN_DEFAULT)
     * @return mixed Null if no row
     */
    public function findOneBy($column, $value, $returnType = Table::RETURN_MODEL) {
        return $this->findOneWhere(array(array($column => $value)), $returnType);
    }

    /**
     * Finds a row by the id column
     * @param mixed $idValue
     * @param int $returnType Indicates the type of return expected. 
     * Possible values (Table::RETURN_MODEL | Table::RETURN_JSON | Table::RETURN_DEFAULT)
     * @return mixed Null if no row
     */
    public function findOne($idValue, $returnType = Table::RETURN_MODEL) {
        if (is_object($idValue)) {
            $getPK = 'get' . ucfirst(U::_toCamel($this->getPrimaryKey()));
            $idValue = $idValue->$getPK();
        }

        return $this->findOneBy($this->getPrimaryKey(), $idValue, $returnType);
    }

    /**
     * Finds a row by the primary column
     * @param mixed $id
     * @param int $returnType Indicates the type of return expected. 
     * Possible values (Table::RETURN_MODEL | Table::RETURN_JSON | Table::RETURN_DEFAULT)
     * @return mixed
     */
    public function find($id, $returnType = Table::RETURN_MODEL) {
        return $this->findBy($this->getPrimaryKey(), $id, $returnType);
    }

    /**
     * Finds row(s) with the given criteria
     * @param array|Mapper $criteria
     * @param int $returnType Indicates the type of return expected. 
     * Possible values (Table::RETURN_MODEL | Table::RETURN_JSON | Table::RETURN_DEFAULT)
     * @return mixed
     */
    public function findWhere($criteria, $returnType = Table::RETURN_MODEL) {
        if (!is_array($criteria)) {
            $criteria = array($criteria);
        }
        $return = $this->select($criteria, $returnType);
        if (is_a($return, 'DBScribe\Table')) $return = $return->execute();
        return $return;
    }

    /**
     * Finds a row with the given criteria
     * @param array|Mapper $criteria
     * @param int $returnType Indicates the type of return expected. 
     * Possible values (Table::RETURN_MODEL | Table::RETURN_JSON | Table::RETURN_DEFAULT)
     * @return mixed
     */
    public function findOneWhere($criteria, $returnType = Table::RETURN_MODEL) {
        $this->limit(1);
        $result = $this->findWhere($criteria, $returnType);
        if (is_array($result)) {
            $result = array_values($result);
            return ($returnType === Table::RETURN_JSON) ? json_encode($result[0])
                        : $result[0];
        }
        else if (is_object($result)) {
            return $result->first();
        }
    }

    public function __call($name, $arguments) {
        try {
            if (strtolower(substr($name, 0, 6)) === 'findby') {
                $column = ucfirst(substr($name, 6));
                return call_user_func_array(array($this, 'findBy'),
                        array_merge(array($column), $arguments));
            }
        }
        catch (Exception $ex) {
            throw new \Exception($ex->getMessage());
        }
    }

    /**
     * Join with given table on every select
     * @see Table::join()
     * @param string $tableName
     * @param array $options
     * @return Repository
     */
    public function alwaysJoin($tableName, array $options = array()) {
        $this->alwaysJoin[$tableName] = $options;
        return $this;
    }

    private function insertJoins() {
        foreach ($this->alwaysJoin as $tableName => $options) {
            $this->join($tableName, $options);
        }
    }

    /**
     * Selects data from database with non-NULL model properties as criteria
     * @param array|Mapper $model a model or an array of models as criteria
     * @param int $returnType Indicates the type of return expected. 
     * Possible values (Table::RETURN_MODEL | Table::RETURN_JSON | Table::RETURN_DEFAULT)
     * @return Repository
     */
    public function select($model = array(), $returnType = Table::RETURN_MODEL) {
        $this->insertJoins();
        if (!is_array($model)) {
            $model = array($model);
        }

        $this->isSelect = true;
        return parent::select($model, $returnType);
    }

    /**
     * Inserts one or more models into the database
     * @param array|Mapper $model a model or an array of models to insert
     * @return Repository
     */
    public function insert($model) {
        if (!is_array($model)) {
            $model = array($model);
        }

        return parent::insert($model);
    }

    /**
     * Updates the database with the model properties
     * @param array|Mapper $model a model or an array of models to insert
     * @param string $whereProperty Property to use as the criterion for the update. Default is "id"
     * @return Repository
     */
    public function update($model, $whereProperty = 'id') {
        if (!is_array($model)) {
            $model = array($model);
        }

        return parent::update($model, $whereProperty);
    }

    /**
     * Deletes data from the database
     * @param array|Mapper $model a model or an array of models to delete
     * @return Repository
     */
    public function delete($model) {
        if ($model) {
            if (!is_array($model)) {
                $model = array($model);
            }
            return parent::delete($model);
        }
        else {
            return parent::delete();
        }
        return $this;
    }

    /**
     * Fetches the name of the table the repository is attached to
     * @return string
     */
    public function getTableName() {
        return $this->getName();
    }

    /**
     * Commits all database transactions
     * @return boolean
     */
    public function flush() {
        return $this->connection->flush();
    }

    public function rollBack() {
        return $this->connection->rollBack();
    }

}
