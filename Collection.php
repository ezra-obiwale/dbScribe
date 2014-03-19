<?php
/*
 */

namespace DBScribe;

/**
 * Description of DBSCollection
 *
 * @author topman
 */
class Collection extends ArrayCollection {

	private $remove;
	private $add;

	/**
	 *
	 * @var \DBScribe\Connection
	 */
	private $connection;

	public function __construct($array = array()) {
		parent::__construct($array);
		$this->remove = $this->add = array();
	}

	/**
	 * Adds a row/model to the collection
	 * @param \DBScribe\Row $value
	 * @return \DBScribe\Collection
	 */
	public function add($value) {
		$this->checkValue($value);
		$this->add[] = $value;
		return parent::add($value);
	}

	/**
	 * Removes a row/model from the collection
	 * @param \DBScribe\Row $value
	 * @return \DBScribe\Collection
	 */
	public function remove($value) {
		$this->checkValue($value);
		$this->remove[] = $value;
		return parent::remove($value);
	}

	private function checkValue($value) {
		if (is_object($value) && is_a($value, 'DBScribe\Row'))
			$return = true;
		else if (is_array($value)) {
			$return = true;
			foreach ($value as $obj) {
				if (!is_object($value) || (is_object($value) && is_a($value, 'DBScribe\Row'))) {
					$return = false;
					break;
				}
			}
		}
		if (!$return)
			throw new \Exception('Param $value must be a subclass of \DBScribe\Row');

		return true;
	}

	/**
	 * Sets the connection object
	 * @param \DBScribe\Connection $connection
	 * @return \DBScribe\Collection
	 */
	public function setConnection(Connection $connection) {
		$this->connection = $connection;
		return $this;
	}

	/**
	 * Sets the content of the collection with an array of array properties and a model
	 * to populate each property array with
	 *
	 * @param array $content Array of arrays to populate into the given model. Each of the
	 * model is then set into the collection. Any existing content is overwritten.
	 * @param \DBScribe\Row $model Model to populate the arrays into
	 * @return \DBScribe\Collection
	 * @throws \Exception
	 */
	public function setContent(array $content, Row $model) {
		foreach ($content as &$modelArray) {
			if (!is_array($modelArray)) {
				throw new \Exception('Each element of param $content must be an array of data to map to the given model');
			}

			$setModel = clone $model;
			$setModel->populate($modelArray);
			$modelArray = $setModel;
		}
		$this->exchangeArray($content);
		return $this;
	}

	/**
	 * Populates the array content of the collection into the given model
	 * @param \DBScribe\Row $model
	 * @return array
	 * @throws \Exception
	 */
	public function populateContent(Row $model) {
		$content = $this->getArrayCopy();
		foreach ($content as &$array) {
			if (!is_array($array)) {
				throw new \Exception('Each element of param $content must be an array of data to map to the given model');
			}

			$setModel = clone $model;
			$setModel->populate($array);
			$array = $setModel;
		}

		return $content;
	}

}
