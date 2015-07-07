<?php

namespace DBScribe;

/**
 * This is a utility class which has some beautiful methods that serve different
 * purposes
 * 
 * @author Ezra Obiwale <contact@ezraobiwale.com>
 */
class Util {

    /**
     * Creates a globally unique 36 character id
     */
    public static function createGUID() {
        if (function_exists('com_create_guid')) {
            return substr(com_create_guid(),1,36);
        }
        else {
            mt_srand((double) microtime() * 10000);
            $charid = strtoupper(md5(uniqid(rand(), true)));
            $hyphen = chr(45);
            $uuid = substr($charid, 0, 8) . $hyphen .
                    substr($charid, 8, 4) . $hyphen .
                    substr($charid, 12, 4) . $hyphen .
                    substr($charid, 16, 4) . $hyphen .
                    substr($charid, 20, 12);

            return $uuid;
        }
    }

    /**
     * Creates a date timestamp
     * @param int|string $time
     * @return string
     */
    public static function createTimestamp($time = null, $format = 'Y-m-d H:i:s') {
        if ($time === null) {
            $time = time();
        }
        elseif (is_string($time)) {
            $time = strtotime($time);
        }

        return date($format, $time);
    }

    /**
     * Turns camelCasedString to under_scored_string
     * @param string $str
     * @return string
     */
    public static function camelTo_($str) {
        if (!is_string($str) || empty($str))
            return '';
        $str[0] = strtolower($str[0]);
        $func = create_function('$c', 'return "_" . strtolower($c[1]);');
        return preg_replace_callback('/([A-Z])/', $func, $str);
    }

    /**
     * Turns under_scored_string to camelCasedString
     * @param string $str
     * @return string
     */
    public static function _toCamel($str) {
        if (!is_string($str))
            return '';
        $func = create_function('$c', 'return strtoupper($c[1]);');
        return preg_replace_callback('/_([a-z])/', $func, $str);
    }

    /**
     * Compresses an array of arrays into a single array merging identical keys
     * into one with their values as an array
     * @param array $array
     * @param string|int|array $keys
     */
    public static function compressArray(array $array, $keys = null) {
        $return = array();

        if ($keys && !is_array($keys)) {
            $keys = array($keys);
        }
        else if (!$keys) {
            $keys = array();
        }

        foreach ($array as $ky => $value) {
            if (!is_array($value) && stristr($value, '__:DS:__')) {
                $value = explode('__:DS:__', $value);
            }

            if (is_array($value)) {
                $return = array_merge_recursive($return, self::compressArray($value, $keys));
                continue;
            }

            if (($keys && in_array($ky, $keys)) || !$keys) {
                if (array_key_exists($ky, $return)) {
                    if (is_array($return[$ky]))
                        $return[$ky][] = $value;
                    else
                        $return[$ky] = array($return[$ky], $value);
                } else {
                    $return[$ky] = $value;
                }
            }
        }
        return $return;
    }

    /**
     * Updates a configuration file
     * @param string $path Full path to the configuration file
     * @param array $data Array of data to add/change in the config
     * @param boolean $recursiveMerge Indicates whether the data should be merge deeply
     * @param array $configArray Array of config to store into path. If this is not NULL,
     * the config file will be overwritten with the array
     * @return boolean
     */
    public static function updateConfig($path, array $data = array(), $recursiveMerge = false, $configArray = null) {
        if (is_null($configArray)) {
            if (is_readable($path))
                $configArray = include $path;
            else {
                $configArray = array();
            }
        }
        $config = ($recursiveMerge) ? array_replace_recursive($configArray, $data) : array_replace($configArray, $data);
        $content = str_replace("=> \n", '=>', var_export($config, true));
        return (file_put_contents($path, '<' . '?php' . "\r\n\treturn " . $content . ';') === FALSE) ? false : true;
    }

    /**
     * Search an array to see if it has the expected value [at the given key]
     * @param array $array
     * @param mixed $value Could be an array of values
     * @param bool $recursive
     * @param mixed $key Could be an array of keys
     * @param bool $multiple Indicates whether to return all found arrays or just one - the first
     * @return array|null The array containing the expected value
     */
    public static function searchArray(array $array, $value, $recursive = false, $key = array(), $multiple = false) {
        if (!is_array($value))
            $value = array($value);
        if ($key !== null && !is_array($key))
            $key = array($key);
        else if ($key === null)
            $key = array();
        $found = array();
        foreach ($array as $ky => $val) {
            if (is_array($val)) {
                if (!$recursive)
                    continue;
                $ret = static::searchArray($val, $value, $recursive, $key, $multiple);
                if (count($ret)) {
                    if ($multiple)
                        $found = array_merge($found, $ret);
                    else
                        return $ret;
                }
                continue;
            }
            if (count($key) && !in_array($ky, $key))
                continue;

            $keys = array_flip($key);
            if ($val === $value[$keys[$ky]]) {
                if ($multiple) {
                    $k = ($multiple === true) ? 0 : $array[$multiple];
                    $found[$k] = $array;
                }
                else
                    return $array;
            }
        }

        return $found;
    }

}
