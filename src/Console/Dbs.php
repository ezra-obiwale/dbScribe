<?php

namespace dbScribe\Console;

use dbScribe\Util,
	dScribe\Core\Console\Script;

/**
 * Description of Dbs
 *
 * @author Ezra Obiwale <contact@ezraobiwale.com>
 */
class Dbs extends Script {

	public static function clearCache() {
		return self::newCommand()
						->shortDescription('Clears the cached database queries')
						->hasOption('--table=TABLENAME', 'Indicates the table to clear');
	}

	public static function _clearCache() {
		$cache_dir = DATA . 'select' . DIRECTORY_SEPARATOR;
		if (array_key_exists('table', self::$options)) {
			$cache = $cache_dir . Util::encode(self::$options['table']) . '.php';
			if (!is_readable($cache)) {
				return self::write('Cache file not found. Could the table name have a prefix?');
			}
			unlink($cache);
		}
		else {
			foreach (scandir($cache_dir) as $cache) {
				if ($cache == '.' || $cache == '..') continue;
				unlink($cache_dir . $cache);
			}
		}
		return self::write('Cache cleared successfully');
	}

}
