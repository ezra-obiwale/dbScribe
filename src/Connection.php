<?php

namespace DBScribe;

/**
 * The class wraps around the PDO class to serve as the connection point to the
 * database, extending the class with some more useful public methods
 *
 * @author Ezra Obiwale <contact@ezraobiwale.com>
 */
class Connection extends \PDO {

    /**
     * Prefix for all tables
     * @var string
     */
    protected $tablePrefix;

    /**
     * Name of database to work with
     * @var string
     */
    protected $dbName;

    /**
     * The Driver Specific Name to use in connection
     * @var string
     */
    protected $dsn;

    /**
     * The username to access the database(s) with
     * @var string
     */
    protected $username;

    /**
     * The password of the user to access the database(s) with
     * @var string
     */
    protected $password;

    /**
     * Options to pass into the PDO Constructor
     * @var array
     */
    protected $options;

    /**
     * Class constructor
     * @param string $dsn
     * @param string $username
     * @param string $password
     * @param array $options
     */
    public function __construct($dsn, $username, $password, $options = array()) {
        $this->dbName = str_replace('dbname=', '', stristr($dsn, 'dbname='));

        if (@$options['create']) {
            parent::__construct(stristr($dsn, ';dbname=', true), $username, $password);
            if ($this->newDB($this->dbName) && $this->dbName) {
                $this->query('use `' . $this->dbName . '`');
            }
        }
        else
            parent::__construct($dsn, $username, $password, $options);

        $this->dsn = $dsn;
        $this->username = $username;
        $this->password = $password;
        $this->options = $options;

        $this->beginTransaction();
    }

    /**
     * Fetches the name of the database in use
     * @return string|null
     */
    public function getDBName() {
        return $this->dbName;
    }

    /**
     * Fetches the prefix for the table
     * @return string|null
     */
    public function getTablePrefix() {
        return @$this->options['tablePrefix'];
    }

    /**
     * Sets the prefix to be used with the tables
     *
     * @param string $prefix
     * @return \DBScribe\Connection
     */
    public function setTablePrefix($prefix) {
        $this->options['tablePrefix'] = $prefix;

        return $this;
    }

    /**
     * Creates a new database
     *
     * @param string|array $dbname
     * @return \DBScibe\Connection|boolean
     */
    public function newDB($dbname) {
        $fQry = "CREATE DATABASE IF NOT EXISTS ";
        $qry = '';
        if (is_string($dbname)) {
            $qry .= $fQry . "`" . $dbname . "`";
        }
        else if (is_array($dbname)) {
            foreach ($dbname as $db) {
                @$qry .= $fQry . "`" . $db . "` ; ";
            }
        }

        return $this->exec($qry);
    }

    /**
     * Drops the current database
     * You'll need to connect to another database to continue
     *
     * @return \DBScibe\Connection|boolean
     */
    public function dropDB() {
        $qry = "DROP DATABASE `" . $this->dbName . "`";
        if ($this->doPrepare($qry)) {
            $this->dsn = stristr($this->dsn, 'dbname=', true);
            return true;
        }
        return false;
    }

    /**
     * Selects another database to work with
     * @param string $dbName
     * @return \DBScribe\Connection
     */
    public function selectDB($dbName) {
        $this->dbName = $dbName;

        $dsn = stristr($this->dsn, 'dbname=', true);
        $this->dsn = ($dsn) ? $dsn . 'dbname=' . $dbName : $this->dsn . 'dbname=' . $dbName;

        parent::__construct($this->dsn, $this->username, $this->password, $this->options);
        return $this;
    }

    /**
     * Creates a table in the database
     * @param \DBScribe\Table $table
     * @param boolean $dropIfExists
     * @return mixed
     * @throws \Exception
     */
    public function createTable(Table &$table, $dropIfExists = true) {
        $qry = '';
        if ($dropIfExists) {
            $qry .= "DROP TABLE IF EXISTS `" . $this->getTablePrefix() . $table->getName() . "`; ";
        }

        $qry .= "CREATE TABLE IF NOT EXISTS `" . $table->getName() . "` (";

        $newColumns = $table->getNewColumns(true);
        if ($table->getNewPrimarykey() && array_key_exists($table->getNewPrimarykey(), $newColumns)) {
            if (!is_string($table->getNewPrimarykey()) || !is_string($newColumns[$table->getNewPrimaryKey()])) {
                throw new \Exception('Column names and information <b>MUST</b> both be strings for table "' .
                $table->getName() . '"');
            }

            $qry .= '`' . $table->getNewPrimarykey() . '` ' . $newColumns[$table->getNewPrimarykey()];
            unset($newColumns[$table->getNewPrimaryKey()]);
            if (count($newColumns))
                $qry .= ', ';
        }

        $cnt = 0;
        foreach ($newColumns as $name => $infoArray) {
            if (!is_string($name) || !is_string($infoArray)) {
                throw new \Exception('Column names and information <b>MUST</b> both be strings for table "' .
                $table->getName() . '"');
            }

            if ($cnt > 0)
                $qry .= ", ";

            $qry .= "`{$name}` {$infoArray}";

            $cnt++;
        }

        if ($table->getNewPrimaryKey())
            $qry .= ', PRIMARY KEY (`' . $table->getNewPrimaryKey(true) . '`)';

        if (count($table->getNewReferences()) > 0) {
            $qry .= ", ";

            $cnt = 1;
            foreach ($table->getNewReferences() as $column => $infoArray) {
                if ($cnt > 1)
                    $qry .= ", ";

                $qry .= "KEY `" . $column . "` (`" . $column . "`)";

                $cnt++;
            }
        }

        if (count($table->getNewIndexes())) {
            $qry .= ', ';

            $cnt = 1;
            foreach ($table->getNewIndexes(true) as $column => $type) {
                if ($cnt > 1)
                    $qry .= ", ";
                $qry .= $type . ' (`' . $column . '`)';
                $cnt++;
            }
        }

        if (!$table->getDescription())
            $table->setDescription();

        if (count($table->getNewReferences()) > 0) {
            foreach ($table->getNewReferences(true) as $column => $infoArray) {
                $qry .= ", FOREIGN KEY (`{$column}`) REFERENCES `" .
                        $this->dbName . '`.`' . $this->getTablePrefix() .
                        $infoArray['table'] . "` (`" . $infoArray['column'] .
                        "`) ON DELETE {$infoArray["onDelete"]} ON UPDATE {$infoArray["onUpdate"]}";
            }
        }

        $qry .= ") " . $table->getDescription() . "; ";
        $return = $this->doPrepare($qry);
        $table->init();
        return $return;
    }

    /**
     * Changes a table in the database
     * @param \DBScribe\Table $table
     * @return mixed
     * @throws \Exception
     * @todo Ensure references are not only removed but replaced when adding
     * onUpdate and\or onDelete values to existing references
     */
    public function alterTable(Table &$table) {
        $alter = 'ALTER TABLE `' . $this->getDBName() . '`.`' . $table->getName() . '`';
        $qry = '';

        if ($table->getNewDescription()) {
            $qry .= $alter . ' ' . $table->getNewDescription(true) . ';';
        }

        if (count($table->getDropReferences())) {
            $qry .= ' ';

            $cnt = 1;
            foreach ($table->getDropReferences(true) as $columnName) {
                $refs = $table->getReferences();

                if (isset($refs[$columnName]) && !empty($refs[$columnName]['constraintName'])) {
                    $qry .= $alter . ' DROP FOREIGN KEY `' . $refs[$columnName]['constraintName'] . '`; ';
                }

                $cnt++;
            }
        }

        if (count($table->getDropIndexes())) {
            $qry .= $alter;
            $cnt = 1;
            $iQry = '';
            foreach ($table->getDropIndexes(true) as $column) {
                if (array_key_exists($column, $table->getIndexes())) {
                    if ($iQry)
                        $iQry .= ',';
                    $iQry .= ' DROP INDEX `' . $table->getIndexes($column) . '`';
                }
                $cnt++;
            }

            $qry .= $iQry . ';';
        }

        $dropColumns = $table->getDropColumns(true);
        if (count($dropColumns)) {
            $qry .= ' ' . $alter;

            $cnt = 1;
            foreach ($dropColumns as $column) {
                if (in_array($column, $table->getColumns(true))) {
                    $qry .= ' DROP COLUMN `' . $column . '`';
                    if ($cnt < count($dropColumns))
                        $qry .= ', ';
                }
                $cnt++;
            }
            $qry .= ';';
        }

        if ($table->shouldDropPrimaryKey(true)) {
            if (!in_array($table->getPrimaryKey(), $dropColumns))
                $qry .= $alter . ' DROP PRIMARY KEY;';
        }

        if (count($table->getNewColumns())) {
            $qry .= ' ' . $alter;
            $qry .= $this->addNewColumns($table->getNewColumns(true), $table, $dropColumns);
            $qry .= ';';
        }


        if (count($table->getAlterColumns())) {
            $query = ' ' . $alter;
            $addNew = array();
            $cnt = 1;
            $addQuery = false;
            $cols = $table->getAlterColumns(true);
            foreach ($cols as $column => $desc) {
                if (in_array($column, $table->getColumns(true))) {
                    $query .= ' CHANGE `' . $column . '` `' . $column . '` ' . $desc;
                    if ($column == $table->getNewPrimarykey())
                        $query .= ' PRIMARY KEY FIRST';
                    if ($cnt < count($cols))
                        $query .= ', ';
                    $addQuery = true;
                } else {
                    $addNew[$column] = $desc;
                }
                $cnt++;
            }
            $query .= ';';

            if (count($addNew)) {
                $qry .= ' ' . $alter;
                $qry .= $this->addNewColumns($addNew, $table);
                $qry .= ';';
            }

            if ($addQuery)
                $qry .= $query;
        }

        $newIndexes = $table->getNewIndexes(true);
        $newIndexesCount = count($newIndexes);
        if ($newIndexes)
            $indexColumns = array_keys($newIndexes);
        if ($newIndexesCount && ($newIndexesCount !== 1 || ($newIndexesCount === 1 && $indexColumns[0] !== $table->getNewPrimarykey()))) {
            $qry .= $alter;

            $cnt = 1;
            foreach ($newIndexes as $column => $type) {
                if ($table->getNewPrimarykey() === $column)
                    continue;

                $qry .= ' ADD ' . $type . ' (`' . $column . '`)';
                if ($cnt < $newIndexesCount)
                    $qry .= ', ';

                $cnt++;
            }

            $qry .= ';';
        }

        if ($table->getNewPrimaryKey()) {
            $qry .= ' ' . $alter . ' ADD PRIMARY KEY (`' .
                    $table->getNewPrimarykey(true) . '`);';
        }

        if (count($table->getNewReferences())) {
            foreach ($table->getNewReferences(true) as $column => $desc) {
                $qry .= ' ' . $alter . " ADD FOREIGN KEY (`{$column}`) REFERENCES `" .
                        $this->dbName . '`.`' . $this->getTablePrefix() .
                        $desc['table'] . "` (`" . $desc['column'] .
                        "`) ON DELETE {$desc["onDelete"]} ON UPDATE {$desc["onUpdate"]}; ";
            }
        }

        $return = $this->doPrepare($qry);
        $table->init();
        return $return;
    }

    private function addNewColumns(array $columns, Table $table, array $dropColumns) {
        $qry = '';
        $cnt = 1;
        foreach ($columns as $column => $desc) {
            if (!in_array($column, $table->getColumns(true)) ||
                    (in_array($column, $table->getColumns(true)) &&
                    in_array($column, $dropColumns))) {
                if ($qry)
                    $qry .= ',';
                $qry .= ' ADD COLUMN `' . $column . '` ' . $desc;
            }
            $cnt++;
        }
        return $qry;
    }

    private function exception(\Exception $ex, $qry, $values) {
        throw new \Exception($ex->getMessage() .
        '<p style="margin-top:20px;"><span style="font-style:italic;color:darkred;background-color:#fff">' .
        $qry . '</span></p><pre>' . print_r($values, true) . '</pre>');
    }

    /**
     * Drop a table from the database
     *
     * @param string|array $tablename
     *
     * @return \DBScibe\Connection|boolean
     */
    public function dropTable($tablename) {
        $qry = "DROP TABLE ";

        if (is_string($tablename)) {
            $qry .= "`" . $this->getTablePrefix() . $tablename . "`";
        }

        try {
            return $this->exec($qry);
        }
        catch (\Exception $ex) {
            $this->exception($ex, $qry);
        }
    }

    /**
     * Creates a table object
     *
     * @param string $tablename Name of table in lower case
     * @param \DBScribe\Row Model to map the table rows to
     * @return \DBScribe\Table
     */
    public function table($tablename, Row $rowModel = null) {
        return new Table($tablename, $this, $rowModel);
    }

    /**
     * Prepares the query and executes against given values, if any
     * @param string $query
     * @param array $values
     * @param array $options Keys include:
     *
     * 		"multipleRows" (boolean)	-	Indicates if values are for multiple rows<br />
     * 		"model" (\Row)				-	The model to morph the results to
     * 		"lastInsertIds" (boolean)			-	Indicates whether to return last insert ids or not
     * @return mixed
     * @throws \Exception
     * @todo use PDO::FETCH_INTO; PDO::FETCH_INTO repeats the last row many times over
     */
    public function doPrepare($query, array $values = null, array $options = array()) {
        try {
            $multipleRows = isset($options['multipleRows']) ? $options['multipleRows'] : false;
            $rowModel = (isset($options['model']) && $options['model']) ? $options['model'] : new Row();
            $retIds = (isset($options['lastInsertIds'])) ?
                    $options['lastInsertIds'] : false;

            if (!$multipleRows && $values !== null) {
                $_values[] = $values;
                $values = $_values;
            }
            $stmt = $this->prepare($query);

            $return = array();
            if ($values) {
                foreach ($values as $vals) {
                    $res = $stmt->execute($vals);
                    $this->createReturn($query, $retIds, $stmt, $res, $return, $vals);
                }
            }
            else {
                $res = $stmt->execute();
                $this->createReturn($query, $retIds, $stmt, $res, $return);
            }

            if (strtolower(substr(ltrim($query), 0, 6)) === 'select') {
                $rowModel->setConnection($this);
                $return = $stmt->fetchAll(\PDO::FETCH_ASSOC);
                return $return;
            }
            else {
                if (count($return) > 1) {
                    return $return;
                }
                else {
                    $ret = array_values($return);
                    return $ret[0];
                }
            }
        }
        catch (\PDOException $ex) {
            $this->exception($ex, $query, $values);
        }
    }

    private function createReturn($query, $retIds, $stmt, $res, &$return, $vals = array()) {
        if (strtolower(substr(ltrim($query), 0, 6)) === 'insert' && $retIds) {
            $return[] = $this->lastInsertId();
        }
//        elseif (strtolower(substr(ltrim($query), 0, 6)) === 'update' && !empty($vals)) {
//            $v = array_values($vals);
//            $return[$v[count($v) - 1]] = $stmt->rowCount();
//        }
        else {
            $return[] = $res;
        }
    }

    /**
     * Commits all changes to the database
     * @return boolean
     */
    public function flush() {
        $return = $this->commit();
        $this->beginTransaction();
        return $return;
    }

    /**
     * Cancels all changes to the database
     * @return boolean
     */
    public function cancel() {
        $return = $this->rollBack();
        $this->beginTransaction();
        return $return;
    }

}
