<?php

namespace dbScribe;

/**
 * Description of Commons
 *
 * @author Ezra
 */
class Commons {

	/**
	 * Returns the name of the class as the string
	 * @return string
	 */
	public function __toString() {
		return get_called_class();
	}

}
