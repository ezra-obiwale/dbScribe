<?php

namespace DBScribe;

use ArrayObject;

/**
 * This is a collection class which has the capacity to hold many rows in an 
 * iteratable manner (like an array) while making available some nice methods 
 * to operate on these rows
 *
 * @author Ezra Obiwale <contact@ezraobiwale.com>
 */
class ArrayCollection extends ArrayObject {

    /**
     *
     * @var int Indicates the current position of the cursor
     */
    private $current;

    /**
     * Gets a parameter from the first entry in the collection if it's an object
     * @param string $name
     * @return mixed
     */
    public function __get($name) {
        if (is_object($this->first()))
            return $this->first()->$name;
    }

    /**
     * Calls a method of the first entry in the collection if it's an object
     * @param string $name
     * @param array $arguments
     * @return mixed
     */
    public function __call($name, $arguments) {
        if (!method_exists($this, $name) && is_object($this->first())) {
            return call_user_func_array(array($this->first(), $name), $arguments);
        }
    }

    /**
     * Fetches the first element in the collection
     * @return mixed
     */
    public function first() {
        if ($this->count())
            $this->current = 0;
        return $this->get(0);
    }

    /**
     * Fetches the last element fetched
     * @return mixed
     */
    public function current() {
        if (is_int($this->current))
            return $this->get($this->current);
    }

    /**
     * Fetches the next, after the last selected, element in the collection
     * @return mixed
     */
    public function next() {
        if (is_int($this->current)) {
            return $this->get($this->current + 1);
        }

        return $this->first;
    }

    /**
     * Fetches the last element in the collection
     * @return mixed
     */
    public function last() {
        if ($this->count()) {
            $this->current = $this->count() - 1;
            return $this->get($this->count() - 1);
        }
    }

    /**
     * Removes a value from the array collection
     * @param mixed $value
     * @return \DBScribe\ArrayCollection
     */
    public function remove($value) {
        $oldCollection = $this->getArrayCopy();
        if (!in_array($value, $oldCollection))
            return $this;

        $newCollection = array();
        foreach ($oldCollection as $val) {
            if ($val === $value)
                continue;

            $newCollection[] = $val;
        }
        $this->exchangeArray($newCollection);
        return $this;
    }

    /**
     * Adds an element to the collection
     * @param mixed $value
     * @return \DBScribe\ArrayCollection
     */
    public function add($value) {
        $this->append($value);
        return $this;
    }

    /**
     * Fetches a value at a given index in the collection
     * @param int $index
     * @return mixed
     */
    public function get($index) {
        if ($this->offsetExists($index)) {
            return $this->offsetGet($index);
        }
    }

    /**
     * Sets a value to an index in the collection. If the index does not exist,
     * it is appended to the collection
     * @param int $index
     * @param mixed $newValue
     * @return \DBScribe\ArrayCollection
     */
    public function set($index, $newValue) {
        if (!$this->offsetSet($index, $newValue)) {
            $this->append($newValue);
        }

        return $this;
    }

    /**
     * Finds all elements that match the given value
     * @param mixed $value
     * @return \DBScribe\ArrayCollection
     */
    public function find($value) {
        $return = new ArrayCollection();
        foreach ($this as $object) {
            if ($value === $object)
                $return->append($object);
        }
        return $return;
    }

    /**
     * Finds the first element that matches the given value
     * @param mixed $value
     * @return mixed
     */
    public function findOne($value) {
        foreach ($this as $object) {
            if ($value === $object)
                return $object;
        }
    }

    /**
     * Finds all objects that the given method returns the given value
     * @param string $method
     * @param mixed $value
     * @param array $methodArgs
     * @return \DBScribe\ArrayCollection
     */
    public function findObjectsByMethod($method, $value, array $methodArgs = array()) {
        $return = new ArrayCollection();
        foreach ($this as $object) {
            if (method_exists($object, $method)) {
                if ($value === call_user_func_array(array($object, $method), $methodArgs))
                    $return->append($object);
            }
        }
        return $return;
    }

    /**
     * Finds the first object that the given method returns the given value
     * @param string $method
     * @param mixed $value
     * @param array $methodArgs
     * @return mixed
     */
    public function findObjectByMethod($method, $value, array $methodArgs = array()) {
        foreach ($this as $object) {
            if (method_exists($object, $method)) {
                if ($value === call_user_func_array(array($object, $method), $methodArgs))
                    return $object;
            }
        }
    }

    /**
     * Finds all objects that the given property has the given value
     * @param string $property
     * @param mixed $value
     * @return \DBScribe\ArrayCollection
     */
    public function findObjects($property, $value) {
        $return = new ArrayCollection();
        foreach ($this as $object) {
            if (property_exists($object, $property)) {
                if ($value === $object->$property)
                    $return->append($object);
            }
        }
        return $return;
    }

    /**
     * Finds the first object that the given property has the given value
     * @param string $property
     * @param mixed $value
     * @return mixed
     */
    public function findObject($property, $value) {
        foreach ($this as $object) {
            if (property_exists($object, $property)) {
                if ($value === $object->$property)
                    return $object;
            }
        }
    }

    public function __toString() {
        return '<pre>' . print_r($this, true) . '</pre>';
    }

}
