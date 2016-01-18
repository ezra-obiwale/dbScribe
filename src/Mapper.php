<?php

namespace DBScribe;

use DBScribe\ArrayCollection,
    DBScribe\Row,
    DBScribe\Table,
    DBScribe\Util,
    DBScribe\Annotation,
    Exception;

/**
 * This class is to be extended by model classes which needs an auto-monitoring
 * on change to be auto-update table and column definitions in the database.
 *
 * @author Ezra Obiwale <contact@ezraobiwale.com>
 */
abstract class Mapper extends Row {
    const ARRAY_COLLECTION_SEPARATOR = '__:DS:__';

    private $settings;

    /**
     *
     * @var \DBScribe\Annotation
     */
    private $annotations;

    /**
     * Initialize the row
     * @param Table $table Live table connection
     */
    public function init(Table &$table) {
        $this->_table = & $table;
        $className = str_replace('\\', '.', get_called_class());
        $path = DATA . 'mapper' . DIRECTORY_SEPARATOR . $className;
        $save = false;

        @session_start();
        if (@$_SESSION['mapperSave'] && !is_readable($path)) {
            $this->save($path, $this->getClassSettings());
            unset($_SESSION['mapperSave']);
        }

        if (!$this->tableExists($table)) {
            $save = $this->createTable($table);
        }
        else {
            $this->checkModelRequirements();
            $save = $this->isUpToDate($path, $table);
        }

        if ($save || !is_readable($path))
            $this->save($path, $this->getClassSettings());
    }

    /**
     * Checks class annotations for requirements
     */
    private function checkModelRequirements() {
        $reload = false;
        $classAnnots = $this->getAnnotations()->getClass();
        if (!empty($classAnnots[1]) && is_array($classAnnots[1])) {
            foreach ($classAnnots[1] as $desc) {
                if (strtolower(substr($desc, 0, 4)) !== 'dbs\\')
                    continue;
                $this->performClassAnnots($desc);
                $reload = true;
            }
        }

        if ($reload) {
            @session_start();
            $_SESSION['mapperSave'] = true;
            header('Location: //' . $_SERVER['SERVER_NAME'] . $_SERVER['REQUEST_URI']);
            exit;
        }
    }

    private function performClassAnnots($desc) {
        $setting = $this->parseSettings($desc, true);
        if (method_exists($this, $setting['type'])) {
            if (!$this->{$setting['type']}($setting['attrs']))
                throw new Exception('DBScribe Class Upgrade "' . $setting['type'] . '" failed');
        }
    }

    /**
     * See Mapper::Autogen()
     * 
     * @param array $attrs
     * @return boolean
     */
    private function Autogenerate(array $attrs) {
        return $this->Autogen($attrs);
    }

    /**
     * Auto generates the class from an existing table in the database
     * @param array $attrs
     * @return boolean
     */
    protected function Autogen(array $attrs) {
        $ignore = !empty($attrs['ignore']) ? explode('|', $attrs['ignore']) : array();
        $er = error_reporting(0);
        ob_start();
        $classPath = $this->openClassForWriting();
        $methods = '';
        foreach ($this->_table->getColumns() as $name => $attrsRow) {
            if (in_array(Util::camelTo_($name), $ignore))
                continue;
            echo "\r\n";
            echo "\t/**\r\n";
            echo "\t * @DBS\\" . $this->fetchColumnProperties($attrsRow) . "\r\n";
            echo "\t */\r\n";
            echo "\tprotected $" . Util::_toCamel($name) . ";\r\n";

            $methods .= "\r\n";
            $methods .= "\tpublic function set" . ucfirst(Util::_toCamel($name)) . '($' . Util::_toCamel($name) . ") {\r\n";
            $methods .= "\t\t" . '$this->' . Util::_toCamel($name) . ' = $' . Util::_toCamel($name) . ";\r\n";
            $methods .= "\t\t" . 'return $this;' . "\r\n";
            $methods .= "\t}\r\n";

            $methods .= "\r\n";
            $methods .= "\tpublic function get" . ucfirst(Util::_toCamel($name)) . "() {\r\n";
            $methods .= "\t\t" . 'return $this->' . Util::_toCamel($name) . ";\r\n";
            $methods .= "\t}\r\n";
        }
        echo $methods . "\r\n";
        echo "}\r\n";
        error_reporting($er);
        return file_put_contents($classPath, ob_get_clean());
    }

    private function fetchColumnProperties($attrsRow) {
        $references = $this->_table->getReferences();
        if ($references[$attrsRow['colName']] && !empty($references[$attrsRow['colName']]['refColumn'])) {
            $ref = $references[$attrsRow['colName']];
            $modelTable = $this->getModelTable($attrsRow['colName']);
            return 'Reference (model="' . @$modelTable[0] .
                    '", property="' . Util::_toCamel($ref['refColumn']) .
                    '", onUpdate="' . $ref['onUpdate'] .
                    '", onDelete="' . $ref['onDelete'] . '", nullable=' .
                    (($attrsRow['nullable'] === 'YES') ? 'true' : 'false') . ')';
        }
        else {
            $exp = explode('(', $attrsRow['colType']);
            if ($exp[0] === 'varchar') {
                $type = 'String';
            }
            else {
                $type = $exp[0];
            }

            $size = isset($exp[1]) ? 'size=' . (int) $exp[1] . ', ' : null;
            return ucfirst($type) . ' (' . $size . $this->fetchColumnAttrs($attrsRow);
        }
    }

    private function fetchColumnAttrs($row) {
        $return = 'nullable=' . (($row['nullable'] === 'YES') ? 'true' : 'false');
        if (!empty($row['colDefault'])) {
            $return .= ', default="' . $row['colDefault'] . '"';
        }
        if ($row['colKey'] === 'PRI') {
            $return .= ', primary=true';
        }
        return $return . ')';
    }

    private function openClassForWriting() {
        $classPath = MODULES . str_replace('\\', DIRECTORY_SEPARATOR, get_called_class()) . '.php';
        foreach (file($classPath) as $line) {
            if (trim($line) === '}')
                continue;

            if (stristr($line, 'DBS\Autogen')) {
                $line = str_ireplace(array('\Autogen', 'ignore'), array('\Autogenerated', 'ignored'), $line);
            }
            echo $line;
        }
        return $classPath;
    }

    private function ignoreThis() {
        $ignore = $this->getIgnore();
        if (!in_array($this->getTableName(), $ignore)) {
            $ignore[] = $this->getTableName();
            @session_start();
            $_SESSION['mapperIgnore'] = $ignore;
        }
        return $this;
    }

    private function ignore() {
        if (in_array($this->getTableName(), $this->getIgnore())) {
            return true;
        }
    }

    /**
     * Checks if the table exists
     * @param Table $table
     * @return boolean
     */
    private function tableExists(Table &$table) {
        return (count($table->getColumns()) > 0);
    }

    /**
     * Creates table in the database from the model's annotations
     * @param Table $table
     * @return mixed
     */
    private function createTable(Table &$table) {
        $annotations = $this->getClassSettings();
        $create = false;
        foreach ($annotations as $columnName => $descArray) {
            $create = true;
            $dbColumnName = Util::camelTo_($columnName);

            if (isset($descArray['attrs']['primary']) && $descArray['attrs']['primary']) {
                $table->setPrimaryKey($dbColumnName);
            }

            if (isset($descArray['attrs']['reference'])) { // Reference not ReferenceMany
                $onDelete = (isset($descArray['attrs']['reference']['onDelete'])) ?
                        $descArray['attrs']['reference']['onDelete'] : 'RESTRICT';
                $onUpdate = (isset($descArray['attrs']['reference']['onUpdate'])) ?
                        $descArray['attrs']['reference']['onUpdate'] : 'RESTRICT';
                $table->addReference($dbColumnName, $descArray['attrs']['reference']['table'], $descArray['attrs']['reference']['column'], $onDelete, $onUpdate);

                $refTable = new Table($descArray['attrs']['reference']['table'], $table->getConnection());
                $refColumns = $refTable->getColumns();
                if (!empty($refColumns[$descArray['attrs']['reference']['column']]['charset'])) {
                    $descArray['attrs']['charset'] = $refColumns[$descArray['attrs']['reference']['column']]['charset'];
                    $descArray['attrs']['collation'] = $refColumns[$descArray['attrs']['reference']['column']]['collation'];
                }

                unset($descArray['attrs']['reference']['table']);
                unset($descArray['attrs']['reference']['column']);
                unset($descArray['attrs']['reference']['model']);

                if (isset($descArray['attrs']['reference']['onUpdate']))
                    unset($descArray['attrs']['reference']['onUpdate']);
                if (isset($descArray['attrs']['reference']['onDelete']))
                    unset($descArray['attrs']['reference']['onDelete']);

                if (!empty($descArray['attrs']['reference'])) {
                    $descArray['attrs'] = array_merge($descArray['attrs'], $descArray['attrs']['reference']);
                    unset($descArray['attrs']['reference']);
                }
            }

            $table->addColumn($dbColumnName, $this->parseAttributes($columnName, $descArray));
            $this->checkIndexes($dbColumnName, $descArray, $table);
        }

        return ($create) ? $table->getConnection()->createTable($table, false) : false;
    }

    /**
     * Prepares attributes of columns for use
     * @param string $columnName
     * @param array $attrs
     * @todo Parse more attributes e.g. unique, index, ... Check what connection->create() can do
     * @return string
     */
    private function parseAttributes($columnName, array $attrs, $isCreate = true) {
        $return = $this->checkType($columnName, $attrs); // type

        if (strtolower($attrs['type']) === 'timestamp' && !isset($attrs['attrs']['default'])) {
            $attrs['attrs']['default'] = '"0000-00-00 00:00:00"';
        }

        if (isset($attrs['attrs']['size']))
            $return .= '(' . $attrs['attrs']['size'] . ')'; // size

        if (isset($attrs['attrs']['collation'])) {
            if (!isset($attrs['attrs']['charset']))
                $attrs['attrs']['charset'] = stristr($attrs['attrs']['collation'], '_', true);

            $return .= ' CHARACTER SET ' . $attrs['attrs']['charset'] . ' COLLATE ' . $attrs['attrs']['collation'];
        }

        $return .= (isset($attrs['attrs']['nullable']) && strtolower($attrs['attrs']['nullable']) == 'true') ? ' NULL' : ' NOT NULL'; // null

        if (isset($attrs['attrs']['default'])) { // auto increment
            if (strtolower($attrs['type']) === 'boolean') {
                if ($attrs['attrs']['default'] === 'true') {
                    $attrs['attrs']['default'] = 1;
                }
                else {
                    $attrs['attrs']['default'] = 0;
                }
            }

            $return .= (trim(strtolower($attrs['type'])) == 'timestamp') ? ' DEFAULT ' . $attrs['attrs']['default'] : ' DEFAULT "' . $attrs['attrs']['default'] . '"';
        }

        if (isset($attrs['attrs']['autoIncrement']) && $attrs['attrs']['autoIncrement'] == 'true') // auto increment
            $return .= ' AUTO_INCREMENT';
        if (!$isCreate) {
            if (!empty($attrs['attrs']['after']))
                $return .= ' AFTER `' . $attrs['attrs']['after'] . '`';
            else if (isset($attrs['attrs']['first']) && $attrs['attrs']['first'])
                $return .= ' FIRST';
        }
        if (isset($attrs['attrs']['onUpdate']) && $attrs['type'] !== 'ReferenceMany') // auto increment
            $return .= ' ON UPDATE ' . $attrs['attrs']['onUpdate'];

        return $return;
    }

    /**
     * Checks the column type to ensure it has required settings
     * @param string $columnName
     * @param array $attrs
     * @return string The type column it is
     */
    private function checkType($columnName, array $attrs) {
        if (in_array(strtolower($attrs['type']), array('string', 'referencemany'))) {
            return (isset($attrs['attrs']['size']) && strtolower($attrs['type']) !== 'referencemany') ? 'VARCHAR' : 'TEXT';
        }
        return strtoupper($attrs['type']);
    }

    private function getClassPath($fullyQualifiedClassName) {
        if (MODULES || VENDOR) {
            return (MODULES && is_readable(MODULES . str_replace('\\', '/', $fullyQualifiedClassName) . '.php')) ?
                    MODULES . str_replace('\\', '/', $fullyQualifiedClassName) . '.php' :
                    VENDOR . str_replace('\\', '/', $fullyQualifiedClassName) . '.php';
        }
        else {
            return dirname(dirname(dirname(dirname(__DIR__)))) .
                    str_replace('\\', '/', $fullyQualifiedClassName) . '.php';
        }
    }

    /**
     * Checks if the table is uptodate with the mapper settings
     * @param string $path Path to save the schema to
     * @param Table $table
     * @return boolean
     */
    private function isUpToDate($path, Table &$table) {
        $classPath = $this->getClassPath(get_called_class());
        $return = null;
        // update if any parent is changed
        foreach (class_parents(get_called_class()) as $parent) {
            if ($parent === get_class())
                break;

            $parent = str_replace(array(
                'DScribe', 'DBScribe', 'DSLive'
                    ), array(
                'd-scribe/core/src', 'd-scribe/db-scribe/src', 'd-scribe/ds-live/src'
                    ), $parent);

            $parentPath = $this->getClassPath($parent);
            if (!is_readable($path) ||
                    (is_readable($path) && is_readable($parentPath) && filemtime($classPath) < filemtime($parentPath))) {
                $return = $this->prepareUpdate($path, $table);
                break;
            }
        }

        if ($return === null && (!is_readable($path) ||
                (is_readable($path) &&
                @filemtime($path) < @filemtime(MODULES . str_replace('\\', '/', get_called_class()) . '.php')))) {
            $return = $this->prepareUpdate($path, $table);
        }

        if (count($this->getCachedSettings()) !== count($table->getColumns(true))) {
            foreach ($this->getModelTable($this->getTableName()) as $class) {
                unlink(DATA . 'mapper' . DIRECTORY_SEPARATOR . str_replace('\\', '.', $class));
            }
//            return $this->isUpToDate($path, $table);
        }
        return ($return === null) ? false : $return;
    }

    /**
     * Updates tables that reference current table
     * @param Table $table
     */
    private function updateReferences(Table &$table) {
        $this->ignoreThis();
        foreach ($table->getBackReferences() as $array) {
            foreach ($array as $ref) {
                if (in_array($ref['refTable'], $this->getIgnore()))
                    continue;

                $refTable = new Table($ref['refTable'], $table->getConnection());
                $modelClass = self::getModelTable($ref['refTable']);
                if (!empty($modelClass)) {
                    $mapper = is_array($modelClass) ?
                            new $modelClass[count($modelClass) - 1] :
                            new $modelClass;
                    $mapper->setConnection($table->getConnection());
                    $mapper->init($refTable, $this->getIgnore());
                }
            }
        }
    }

    /**
     * Prepares to update the table with the settings in the mapper class
     * @param string $path Pathh to save the schema to
     * @param Table $table
     * @return void
     */
    private function prepareUpdate($path, Table &$table) {
        $newDesc = $this->getClassSettings(true);
        // to indicate whether oldDesc directly from Table in DB
        $liveDesc = false;

        if (is_readable($path)) {
            $oldDesc = include $path;
        }
        else {
            $oldDesc = array();
            if ($table->exists()) {
                $liveDesc = true;
                foreach ($table->getColumns() as $columnName => $info) {
                    $columnName = Util::_toCamel($columnName);
                    $type = ucfirst(preg_filter('/[^a-zA-Z]/', '', $info['colType']));
                    $size = preg_filter('/[^0-9]/', '', $info['colType']);
                    switch (strtolower($type)) {
                        case "varchar":
                            if (array_key_exists($columnName, $newDesc) && strtolower($newDesc[$columnName]['type']) === 'string')
                                $type = $newDesc[$columnName]['type'];
                            break;
                        case "tinyint":
                            if (array_key_exists($columnName, $newDesc) && in_array(strtolower($newDesc[$columnName]['type'], array('boolean', 'tinyint', 'string'))))
                                $type = $newDesc[$columnName]['type'];
                            else
                                $type = 'Boolean';
                            break;
                    }
                    $oldDesc[$columnName] = array(
                        'type' => $type,
                        'attrs' => array()
                    );

                    if (strtolower($type) !== 'boolean')
                        $oldDesc[$columnName]['attrs']['size'] = $size;

                    if ($info['nullable'] == 'Yes')
                        $oldDesc[$columnName]['attrs']['nullable'] = 'true';
                    else if (array_key_exists($columnName, $newDesc) && @$newDesc[$columnName]['attrs']['nullable'] == 'false') {
                        $oldDesc[$columnName]['attrs']['nullable'] = 'false';
                    }

                    if (!empty($info['colDefault']))
                        $oldDesc[$columnName]['attrs']['default'] = $info['colDefault'];

                    switch ($info['colKey']) {
                        case 'PRI':
                            $oldDesc[$columnName]['attrs']['primary'] = 'true';
                            break;
                        case 'UNI':
                            $oldDesc[$columnName]['attrs']['unique'] = 'true';
                            break;
                        case 'MUL':
                            $oldDesc[$columnName]['attrs']['index'] = 'true';
                            break;
                    }
                }
            }
        }

        $toDo = array('new' => array(), 'update' => array(), 'remove' => array());

        foreach ($newDesc as $property => $annotArray) {
            if (!$property)
                continue;
            $_property = Util::camelTo_($property);
            if (!isset($oldDesc[$property])) {
                $toDo['new'][$_property] = $annotArray;
                unset($oldDesc[$property]);
            }
            else if (count($annotArray) != count($oldDesc[$property])) {
                $toDo['update'][$_property] = $annotArray;
                unset($oldDesc[$property]);
            }
            elseif (count($annotArray) === count($oldDesc[$property])) {
                foreach ($annotArray as $attr => $value) {
                    if (!isset($oldDesc[$property][$attr]) ||
                            (isset($oldDesc[$property][$attr]) && $value !== $oldDesc[$property][$attr])) {
                        $toDo['update'][$_property] = $annotArray;
                        unset($oldDesc[$property]);
                        break;
                    }
                }
                unset($oldDesc[$property]);
            }
        }

        $toDo['remove'] = array_map(function($ppt) {
            return Util::camelTo_($ppt);
        }, array_keys($oldDesc));

        return $this->updateTable($toDo, $table);
    }

    /**
     * Updates the table
     * @param array $columns
     * @param Table $table
     * @return mixed
     */
    private function updateTable(array $columns, Table &$table) {
        $this->updateReferences($table);
        $canUpdate = false;
        foreach ($columns['remove'] as $columnName) {
            $canUpdate = true;
            $table->dropColumn(Util::camelTo_($columnName));
        }

        foreach ($columns['update'] as $columnName => &$desc) {
            $canUpdate = true;
            $dbColumnName = Util::camelTo_($columnName);

            $this->checkIndexes($dbColumnName, $desc, $table);

            if (isset($desc['attrs']['reference'])) {
                $onDelete = (isset($desc['attrs']['reference']['onDelete'])) ?
                        $desc['attrs']['reference']['onDelete'] : 'RESTRICT';
                $onUpdate = (isset($desc['attrs']['reference']['onUpdate'])) ?
                        $desc['attrs']['reference']['onUpdate'] : 'RESTRICT';
                $table->alterReference($dbColumnName, $desc['attrs']['reference']['table'], $desc['attrs']['reference']['column'], $onDelete, $onUpdate);
                unset($desc['attrs']['reference']['table']);
                unset($desc['attrs']['reference']['column']);
                unset($desc['attrs']['reference']['model']);

                if (isset($desc['attrs']['reference']['onUpdate']))
                    unset($desc['attrs']['reference']['onUpdate']);
                if (isset($desc['attrs']['reference']['onDelete']))
                    unset($desc['attrs']['reference']['onDelete']);

                if (!empty($desc['attrs']['reference'])) {
                    $desc['attrs'] = array_merge($desc['attrs'], $desc['attrs']['reference']);
                    unset($desc['attrs']['reference']);
                }
            }

            $table->alterColumn($dbColumnName, $this->parseAttributes($columnName, $desc, false));

            if (isset($desc['attrs']['primary']) && $desc['attrs']['primary'])
                $table->setPrimaryKey($dbColumnName);
        }

        foreach ($columns['new'] as $columnName => $desc) {
            $canUpdate = true;
            $dbColumnName = Util::camelTo_($columnName);

            if (isset($desc['attrs']['reference'])) {
                $onDelete = (isset($desc['attrs']['reference']['onDelete'])) ?
                        $desc['attrs']['reference']['onDelete'] : 'RESTRICT';
                $onUpdate = (isset($desc['attrs']['reference']['onUpdate'])) ?
                        $desc['attrs']['reference']['onUpdate'] : 'RESTRICT';
                $table->addReference($dbColumnName, $desc['attrs']['reference']['table'], $desc['attrs']['reference']['column'], $onDelete, $onUpdate);
                unset($desc['attrs']['reference']['table']);
                unset($desc['attrs']['reference']['column']);
                unset($desc['attrs']['reference']['model']);

                if (isset($desc['attrs']['reference']['onUpdate']))
                    unset($desc['attrs']['reference']['onUpdate']);
                if (isset($desc['attrs']['reference']['onDelete']))
                    unset($desc['attrs']['reference']['onDelete']);

                if (!empty($desc['attrs']['reference'])) {
                    $desc['attrs'] = array_merge($desc['attrs'], $desc['attrs']['reference']);
                    unset($desc['attrs']['reference']);
                }
            }

            $table->addColumn($dbColumnName, $this->parseAttributes($columnName, $desc, false));

            $this->checkIndexes($dbColumnName, $desc, $table);

            if (isset($desc['attrs']['primary']) && $desc['attrs']['primary'])
                $table->setPrimaryKey($dbColumnName);
        }

        if ($canUpdate) {
            $droppedRefs = array();
            foreach ($table->getBackReferences() as $refArray) {
                foreach ($refArray as $ref) {
                    $refTable = new Table($ref['refTable'], $table->getConnection());
                    foreach ($refTable->getReferences() as $col => $array) {
                        if ($refTable->getName() !== $table->getName() && $array['refTable'] === $table->getName()) {
                            $refTable->dropReference($col);
                            $droppedRefs[$col][] = array_merge(array(
                                'table' => $refTable,
                                    ), $array);

                            $table->getConnection()->alterTable($refTable);
                        }
                    }
                }
            }
            $return = $table->getConnection()->alterTable($table);

            foreach ($droppedRefs as $col => $array) {
                foreach ($array as $ref) {
                    $onDelete = (!empty($ref['onDelete'])) ? $ref['onDelete'] : 'RESTRICT';
                    $onUpdate = (!empty($ref['onUpdate'])) ? $ref['onUpdate'] : 'RESTRICT';
                    $ref['table']->addReference($col, $ref['refTable'], $ref['refColumn'], $onDelete, $onUpdate);

                    $table->getConnection()->alterTable($ref['table']);
                }
            }
            if ($return) {
                $table->getConnection()->flush();
                return true;
            }
        }

        return false;
    }

    /**
     * Checks if a column should be indexed, removing old index and adding new
     * @param string $dbColumnName
     * @param array $desc
     * @param Table $table
     * @return void
     */
    private function checkIndexes($dbColumnName, array $desc, Table &$table) {
        // if column is not primary key
        if ($table->getPrimaryKey() !== $dbColumnName) {
            // drop index if column is indexed
            if (array_key_exists($dbColumnName, $table->getIndexes())) {
                // drop reference if column is referenced
                if (array_key_exists($dbColumnName, $table->getReferences())) {
                    $table->dropReference($dbColumnName);
                }
                $table->dropIndex($dbColumnName);
            }

            // add index to column if described as such
            if (isset($desc['attrs']['index'])) {
                $table->addIndex($dbColumnName, Table::INDEX_REGULAR);
            }
            else if (isset($desc['attrs']['unique'])) {
                $table->addIndex($dbColumnName, Table::INDEX_UNIQUE);
            }
            else if (isset($desc['attrs']['fulltext'])) {
                $table->addIndex($dbColumnName, Table::INDEX_FULLTEXT);
            }
            else {
                return null;
            }

            $ref = $table->getReferences();
            if (array_key_exists($dbColumnName, $ref) && !empty($ref[$dbColumnName]['refTable'])) {
                $table->addReference($dbColumnName, $ref[$dbColumnName]['refTable'], $ref[$dbColumnName]['refColumn'], $ref[$dbColumnName]['onDelete'], $ref[$dbColumnName]['onUpdate']);
            }
        }
    }

    /**
     * Replaces the magic method __call() for mapper classes
     * @param string $name
     * @param array $arguments
     */
    protected function _preCall(&$name, array &$args) {
        if (!method_exists($this, $name)) {
            $settings = $this->getCachedSettings($name);
            if (array_key_exists('reference', $settings['attrs']) || $settings['type'] == 'ReferenceMany') {
                $modelTable = array_key_exists('model', $settings['attrs']) ?
                        $settings['attrs']['model'] :
                        $settings['attrs']['reference']['model'];
                $columnValue = is_object($this->$name) ? $this->$name->getArrayCopy() : $this->$name;
                $nam = !is_array($columnValue) ? explode('__:DS:__', $columnValue) : $columnValue;
                $column = array_key_exists('column', $settings['attrs']) ?
                        $settings['attrs']['column'] :
                        $settings['attrs']['reference']['column'];
                $args['relateWhere'] = array();
                foreach ($nam as $val) {
                        $args['relateWhere'][] = array($column => $val);
                }
            }
            if ($modelTable || (!$modelTable && $modelTable = self::getModelTable(Util::camelTo_($name)))) {
                $model = is_array($modelTable) ?
                        new $modelTable[count($modelTable) - 1] :
                        new $modelTable;

                $name = $model->getTableName();
                if (!isset($args[0]['returnType']) || $args[0]['returnType'] == Table::RETURN_MODEL){
                    $relTable = $this->getConnection()->table($name);

                    $model->setConnection($this->getConnection());
                    $model->init($relTable);
                    $args['model'] = $model;
                }
            }
        }
    }

    /**
     * Parses the annotations to bring out the required
     * @param array $annotations
     * @return array
     */
    private function parseAnnotations(array $annotations, $createReference = true) {
        $return = $defer = $primary = array();
        $first = $prev = null;
        foreach ($annotations as $property => $annotArray) {
            if (!is_array($annotArray))
                continue;

            foreach ($annotArray as &$desc) {
                $desc = $this->parseSettings(substr($desc, 1));
                if ($prev && !$desc['attrs']['primary']) {
                    $desc['attrs']['after'] = $prev;
                }
                else if (!$prev && !$desc['attrs']['primary']) {
                    $desc['attrs']['first'] = true;
                    $first = $property;
                }

                $prev = Util::camelTo_($property);
                if (strtolower($desc['type']) === 'reference' || strtolower($desc['type']) === 'referencemany') {
                    $defer[$property] = $desc;
                }

                if (isset($desc['attrs']['primary']) && $desc['attrs']['primary']) {
                    $primary['column'] = $property;
                    $desc['attrs']['first'] = true;
                    if ($first) {
                        $return[$first]['attrs']['after'] = Util::camelTo_($property);
                        unset($return[$first]['attrs']['first']);
                        $first = $property;
                    }
                    $primary['desc'] = $desc;
                }

                $return[$property] = $desc;
                break;
            }
        }

        foreach ($defer as $property => &$desc) {
            $desc = $this->parseForReference($property, $desc, $primary, $createReference);
            if ($desc['attrs']['first'] && !$return[$property]['attrs']['first'])
                unset($desc['attrs']['first']);
            if ($return[$property]['attrs']['after'])
                $desc['attrs']['after'] = $return[$property]['attrs']['after'];
        }

        $f = array();
        if ($first) {
            $f[$first] = $return[$first];
            unset($return[$first]);
        }

        return array_merge($f, $return, $defer);
    }

    private function parseForReference($property, $oDesc, $primary, $createReference) {
        $desc = $this->createReference($property, $oDesc, $primary, $createReference);
        if (strtolower($oDesc['type']) === 'referencemany') {
            $desc['attrs'] = array_merge($oDesc['attrs'], $desc['attrs']['reference']);

            unset($desc['attrs']['size']);
            unset($desc['attrs']['reference']);
            $desc['type'] = 'ReferenceMany';
        }
        return $desc;
    }

    /**
     * Parses the settings for all columns
     * @param string $annot
     * @return array
     */
    private function parseSettings($annot) {
        $annot = str_ireplace(array(' (', ', ', ', ', ')', '"', "'"), array('(', ', ', ', '), $annot);
        $exp = preg_split('[\(]', $annot);
        $return = array(
            'type' => str_ireplace('dbs\\', '', $exp[0]),
            'attrs' => array(),
        );
        if (isset($exp[1])) {
            parse_str(str_replace(array(','), array('&'), $exp[1]), $return['attrs']);
        }

        $_return = $return;
        foreach ($_return['attrs'] as $attr => $val) {
            $return['attrs'][$attr] = trim($val);
        }
        return $return;
    }

    /**
     * Fetches the settings for a property
     * @param string $property
     * @return mixed
     */
    final public function getCachedSettings($property = null) {
        if ($this->settings === null) {
            $path = DATA . 'mapper' . DIRECTORY_SEPARATOR . str_replace('\\', '.', get_called_class());
            $this->settings = include $path;
        }

        if (is_array($this->settings) && isset($this->settings[$property]))
            return $this->settings[$property];

        return $this->settings;
    }

    private function checkModelExists($model) {
        if (!strstr($model, '\\')) {
            $nm = $this->getNamespace();
            $model = $nm . '\\' . $model;
            if (class_exists($model))
                return $model;
        }

        if (class_exists($model))
            return $model;

        return false;
    }

    final public function getNamespace() {
        $exp = explode('\\', get_called_class());
        unset($exp[count($exp) - 1]);
        return join('\\', $exp);
    }

    /**
     * Creates the reference settings for a reference column
     * @param string $property
     * @param array $annot
     * @param array $primary If available, will contain keys "column" and "desc"
     * indicating the primary column and it's description
     * @return array
     * @throws Exception
     * @todo allow referencing table with no model
     */
    private function createReference($property, array $annot, array $primary = array()) {
        $this->ignoreThis();
        if (!isset($annot['attrs']['model']))
            throw new Exception('Attribute "model" not set for reference property "' .
            $property . '" of class "' . get_called_class() . '"');

        if (!$annot['attrs']['model'] = $this->checkModelExists($annot['attrs']['model'])) {
            throw new Exception('Model "' . $annot['attrs']['model'] . '" of reference property "' . $property .
            '" does not exist in class "' . get_called_class() . '"');
        }
        elseif (!in_array('DScribe\Core\IModel', class_implements($annot['attrs']['model'])))
            throw new Exception('Model "' . $annot['attrs']['model'] . '" must implement "DScribe\Core\IModel"');

        $refTable = new $annot['attrs']['model'];

        if ($this->getConnection()->getTablePrefix() . $refTable->getTableName() === $this->_table->getName() && !empty($primary['column'])) {
            $annot['attrs']['property'] = Util::camelTo_($primary['column']);
            $attrs = $primary['desc'];
            $conColumns = $this->_table->getColumns();
        }
        else {
            $refTable->setConnection($this->_table->getConnection());
            $conTable = $this->_table->getConnection()->table($refTable->getTableName(), $refTable);
            $refTable->init($conTable);
            if (!isset($annot['attrs']['property'])) {
                if (!$conTable->getPrimaryKey())
                    throw new Exception('Property "' . $property . '" must have attribute "property" as model "' .
                    $annot['attrs']['model'] . '" does not have a primary key');

                $annot['attrs']['property'] = $conTable->getPrimaryKey();
            }

            $annot['attrs']['property'] = \Util::camelTo_($annot['attrs']['property']);

            if (!array_key_exists($annot['attrs']['property'], $conTable->getIndexes())) {
                $conTable->addIndex($annot['attrs']['property']);
                if ($conTable->exists()) {
                    $this->_table->getConnection()->alterTable($conTable);
                }
            }
            $attrs = $refTable->getCachedSettings(\Util::_toCamel($annot['attrs']['property']));
            $conColumns = $conTable->getColumns();
        }
        if ($attrs === null)
            throw new Exception('Property "' . $annot['attrs']['property'] . '", set as attribute "property" for property "' .
            $property . '" in class "' . get_called_class() . '", does not exist');

        if (isset($attrs['attrs']['auto_increment']))
            unset($attrs['attrs']['auto_increment']);
        if (isset($attrs['attrs']['primary']))
            unset($attrs['attrs']['primary']);

        $column = $annot['attrs']['property'];
        unset($annot['attrs']['property']);
        unset($annot['attrs']['size']);
        unset($annot['attrs']['first']);
        unset($annot['attrs']['after']);

        if (!empty($conColumns[$column]['charset']))
            $attrs['attrs']['charset'] = $conColumns[$column]['charset'];
        if (!empty($conColumns[$column]['collation']))
            $attrs['attrs']['collation'] = $conColumns[$column]['collation'];

        $attrs['attrs']['reference'] = array_merge($annot['attrs'], array(
            'table' => $refTable->getTableName(),
            'column' => $column,
        ));

        return $attrs;
    }

    /**
     * Saves the annotations as table schema
     * @param string $path
     * @param array $annotations
     */
    private function save($path, $annotations) {
        if (!is_dir(DATA . 'mapper'))
            mkdir(DATA . 'mapper', 0755, TRUE);

        $content = var_export($annotations, true);
        file_put_contents($path, '<' . '?php' . "\n\t" . 'return ' . str_replace("=> \n", ' => ', $content) . ";");

        $this->saveModelTable($this->getTableName(), get_called_class());
    }

    /**
     * Saves table models
     * @param string $tableName
     * @param string $modelClass
     * @return boolean
     */
    private function saveModelTable($tableName, $modelClass) {
        $modelTables = array();
        $mt = DATA . 'mapper' . DIRECTORY_SEPARATOR . '__modelTables.php';
        if (is_readable($mt))
            $modelTables = include $mt;

        if (!$modelTables[$tableName] || ($modelTables[$tableName] && is_a($this, $modelTables[$tableName]))) {
            $modelTables[$tableName] = $modelClass;
            return file_put_contents($mt, '<' . '?php' . "\n\t" . 'return ' . stripslashes(var_export($modelTables, true)) . ";");
        }

        return true;
    }

    /**
     * Fetches the model class for the given table name
     * @param string $tableName
     * @return array
     */
    public static function getModelTable($tableName) {
        $mt = DATA . 'mapper' . DIRECTORY_SEPARATOR . '__modelTables.php';
        if (!is_readable($mt))
            return array();

        $modelTables = include $mt;
        if (isset($modelTables[$tableName]))
            return $modelTables[$tableName];

        return array();
    }

    /**
     * Function to call before saving the model
     *
     * Cleans up values of types date, time and timestamp
     *
     * Turns empty values to null
     */
    public function preSave() {
        $this->getCachedSettings();
        foreach ($this->toArray() as $ppt => $val) {
            $settingKey = \Util::_toCamel($ppt);
            if (@$this->settings[$settingKey]['type'] === 'ReferenceMany' && !empty($val)) {
                if (is_object($val) && is_a($val, 'DBScribe\ArrayCollection')) {
                    $val = $val->getArrayCopy();
                }

                $idSep = '__:DS:__';

                $ids = '';
                if (is_array($val)) {
                    foreach ($val as $vl) {
                        if (is_object($vl) && is_a($vl, 'DBScribe\Row')) {
                            if (!isset($vl->id)) {
                                throw new Exception('ReferenceMany requires that all objects must have an ID');
                            }
                            if (method_exists($vl, 'getId')) {
                                $vl = $vl->getId();
                            }
                            else {
                                $vl = $vl->id;
                            }
                        }

                        if (is_null($vl)) {
                            throw new Exception('ReferenceMany objects CANNOT have a null ID');
                        }

                        $ids .= (!empty($ids)) ? $idSep . $vl : $vl;
                    }

                    $this->$settingKey = $ids;
                    $val = $this->$settingKey;
                }
            }
            else if (strtolower(@$this->settings[$settingKey]['type']) === 'date' && !empty($val) && !strstr($val, '0000')) {
                $this->$settingKey = Util::createTimestamp(strtotime($val), 'Y-m-d');
                $val = $this->$settingKey;
            }
            elseif (strtolower(@$this->settings[$settingKey]['type']) === 'time' && !empty($val) && !strstr($val, '0000')) {
                $this->$settingKey = Util::createTimestamp(strtotime($val), 'H:i');
                $val = $this->$settingKey;
            }
            elseif (strtolower(@$this->settings[$settingKey]['type']) === 'timestamp' && !empty($val) && !strstr($val, '0000')) {
                $this->$settingKey = Util::createTimestamp(strtotime($val), 'Y-m-d H:i:s');
                $val = $this->$settingKey;
            }

            if (empty($val) && $val != 0) {
                if (!property_exists($this, $ppt))
                    $ppt = Util::_toCamel($ppt);
                $this->$ppt = null;
            }
        }

        if (!$this->getConnection() && $this->_table) {
            $this->setConnection($this->_table->getConnection());
            $table = $this->_table->getConnection()->table($this->getTableName());
            $this->init($table);
        }

        parent::preSave();
    }

    /**
     * Function to call after fetching row
     * 
     * Straightens out ReferenceMany into ArrayCollection
     */
    public function postFetch($property = null) {
        if (!empty($this->settings)) {
            foreach ($this->settings as $column => $descArray) {
                if ($descArray['type'] === 'ReferenceMany') {
                    if (is_string($this->$column) && $newValue = json_decode($this->$column,  true))
                            $this->$column = $newValue;
                    $this->$column = new ArrayCollection($this->$column ?
                            explode(self::ARRAY_COLLECTION_SEPARATOR, $this->$column): array());
                }
            }
        }

        parent::postFetch();
    }

    private function getIgnore() {
        @session_start();
        return is_array($_SESSION['mapperIgnore']) ? $_SESSION['mapperIgnore'] : array();
    }

    /**
     * Retrieves the annotations in the mapper class
     * @param boolean $forceNew Indicates whether to regenerate settings ignore existing one
     * @return array
     */
    private function getAnnotations($forceNew = false) {
        if ($this->annotations === null || $forceNew === true) {
            $this->annotations = new Annotation(get_called_class());
        }

        return $this->annotations;
    }

    private function getClassSettings($forceNew = false) {
        return $this->parseAnnotations($this->getAnnotations($forceNew)
                ->getProperties('DBS'), !$this->ignore());
    }

    /**
     * Replaces the magic method __set() for mapper classes
     * @param string $property
     * @param mixed $value
     */
    protected function _set($property, $value) {
        
    }

    /**
     * Serializes only the properties of the model
     * @return type
     */
    public function __sleep() {
        return array_keys($this->toArray(false, true));
    }

    /**
     * Returns the name of the class as the string
     * @return string
     */
    public function __toString() {
        return get_called_class();
    }

    final public function getTable() {
        return $this->_table;
    }

    /**
     * Fetches an array of properties and their values
     * @param boolean $withNull Indicates whether to return properties with null values too
     * @param boolean $asIs Indicates whether to return properties as gotten from parent method.
     * This leaves them with underscores and not camel cases
     * @return array
     */
    public function toArray($withNull = false, $asIs = false) {
        $array = parent::toArray();
        unset($array['_table']);

        if ($asIs) {
            return $array;
        }

        $return = array();
        foreach ($array as $name => $value) {
            if (($value === null && !$withNull)) {
                continue;
            } else if (is_object($value) && is_a($value, 'DBScribe\ArrayCollection'))
                $value = $value->getArrayCopy();
            $return[\Util::camelTo_($name)] = $value;
        }
        return $return;
    }

}
