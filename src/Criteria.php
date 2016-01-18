<?php

/*
 * The MIT License
 *
 * Copyright 2015 ezra.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

namespace DBScribe;

/**
 * This class is meant to replace arrays as criteria
 *
 *
 * @author Ezra Obiwale <contact@ezraobiwale.com>
 * @todo Implement in whole system
 */
class Criteria {

    protected $query;
    protected $dummyQuery;
    protected $values;
    protected $tableName;
    protected $groups;

    public function __construct() {
        $this->tableName = ':TBL:';
        $this->groups = 0;
        $this->query = '(';
    }

    /**
     * Set the name of the table for which the query criteria is for
     * @param string $tableName
     * @return \DBScribe\Criteria
     */
    public function setTableName($tableName) {
        $this->query = str_replace($this->tableName, $tableName, $this->query);
        $this->dummyQuery = str_replace($this->tableName, $tableName, $this->dummyQuery);
        $this->tableName = $tableName;
        return $this;
    }

    /**
     *
     * @param string $name
     * @param array $arguments
     * @return Criteria
     */
    public function __call($name, $arguments) {
        if (!method_exists($this, $name)) {
            if (strtolower(substr($name, 0, 3)) === 'and') {
                $method = substr($name, 3);
                if (method_exists($this, $method)) {
                    $this->query .= ' AND ';
                    $this->dummyQuery .= ' AND ';
                    return call_user_func_array(array($this, $method), $arguments);
                }
            } else if (strtolower(substr($name, 0, 2)) === 'or') {
                $method = substr($name, 2);
                if (method_exists($this, $method)) {
                    $this->query .= ' OR ';
                    $this->dummyQuery .= ' OR ';
                    return call_user_func_array(array($this, $method), $arguments);
                }
            }
        }
    }

    private function doGroup($logical) {
        if ($this->query) {
            $this->query .= ' ' . $logical . ' (';
            $this->dummyQuery .= ' ' . $logical . ' (';
            $this->groups++;
        }
        return $this;
    }

    /**
     * Start grouping criteria and join to the query with AND
     * @return \DBScribe\Criteria
     */
    public function andGroup() {
        return $this->doGroup('AND');
    }

    /**
     * Start grouping criteria and join to the query with OR
     * @return \DBScribe\Criteria
     */
    public function orGroup() {
        return $this->doGroup('OR');
    }

    /**
     * End criteria grouping
     * @return \DBScribe\Criteria
     */
    public function endGroup() {
        if ($this->groups) {
            $this->query .= ')';
            $this->dummyQuery .= ')';
            $this->groups--;
        }
        return $this;
    }

    private function doOperand($column, $operand, $value = NULL, $operand2 = NULL, $value2 = NULL) {
        $this->query .= '`' . $this->tableName . '`.`' . $column . '` ' . $operand;
        if ($value) {
            $this->query .= ' ?';
            if (!is_null($value) && !is_array($value)) {
                $this->values[] = $value;
                $value = is_string($value) ? '"' . $value . '"' : $value;
                if ($operand2) {
                    $this->query .= ' ' . $operand2 . ' ' . $value2;
                    $value2 = is_string($value2) ? '"' . $value2 . '"' : $value2;
                }
            } else if (!is_null($value)) {
                $this->values = array_merge($this->values, $value);
                $value = '("' . join('", "', $value) . '")';
            }
        }
        $this->dummyQuery .= '`' . $this->tableName . '`.`' . $column . '` ' . $operand;
        if (!is_null($value)) {
            $this->dummyQuery .= ' ' . $value;
            if ($operand2)
                $this->dummyQuery .= ' ' . $operand2 . ' ' . $value2;
        }
        return $this;
    }

    /**
     * @param string $column
     * @param mixed $value
     * @return \DBScribe\Criteria
     */
    public function equal($column, $value) {
        return $this->doOperand($column, '=', $value);
    }

    /**
     * @param string $column
     * @param mixed $value
     * @return \DBScribe\Criteria
     */
    public function notEqual($column, $value) {
        return $this->doOperand($column, '!=', $value);
    }

    /**
     * @param string $column
     * @param mixed $value
     * @return \DBScribe\Criteria
     */
    public function like($column, $value) {
        return $this->doOperand($column, 'LIKE', $value);
    }

    /**
     * @param string $column
     * @param mixed $value
     * @return \DBScribe\Criteria
     */
    public function notLike($column, $value) {
        return $this->doOperand($column, 'NOT LIKE', $value);
    }

    /**
     * @param string $column
     * @param mixed $value
     * @return \DBScribe\Criteria
     */
    public function regExp($column, $value) {
        return $this->doOperand($column, 'REGEXP', $value);
    }

    /**
     * @param string $column
     * @param mixed $value
     * @return \DBScribe\Criteria
     */
    public function notRegExp($column, $value) {
        return $this->doOperand($column, 'NOT REGEXP', $value);
    }

    /**
     * @param string $column
     * @return \DBScribe\Criteria
     */
    public function isNull($column) {
        return $this->doOperand($column, 'IS NULL');
    }

    /**
     * @param string $column
     * @return \DBScribe\Criteria
     */
    public function isNotNull($column) {
        return $this->doOperand($column, 'IS NOT NULL');
    }

    /**
     * @param string $column
     * @param array $value
     * @return \DBScribe\Criteria
     */
    public function in($column, array $value) {
        return $this->doOperand($column, 'IN', $value);
    }

    /**
     * @param string $column
     * @param array $value
     * @return \DBScribe\Criteria
     */
    public function notIn($column, array $value) {
        return $this->doOperand($column, 'NOT IN', $value);
    }

    /**
     * @param string $column
     * @param mixed $value1
     * @param mixed $value2
     * @return \DBScribe\Criteria
     */
    public function between($column, $value1, $value2) {
        return $this->doOperand($column, 'BETWEEN', $value1, 'AND', $value2);
    }

    /**
     * @param string $column
     * @param mixed $value1
     * @param mixed $value2
     * @return \DBScribe\Criteria
     */
    public function notBetween($column, $value1, $value2) {
        return $this->doOperand($column, 'NOT BETWEEN', $value1, 'AND', $value2);
    }

    /**
     *
     * @param boolean $parseValues Indicates whether to insert values in query (TRUE)
     * or show the prepared query (FALSE)
     * @return string
     */
    public function getQuery($parseValues = false) {
        return $parseValues ? $this->dummyQuery : $this->query . ')';
    }

    /**
     * Fetches the array of values used in query
     * @return array
     */
    public function getValues() {
        return $this->values;
    }

}
