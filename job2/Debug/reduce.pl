#!/usr/bin/perl
# This script takes data with the folowing format ordered by DATE;WBAN and returns the day, the WBAN, the greater temperature and what time that measure was sampled
# Input (ordered!) DATE;WBAN;TEMP;TIME
# Output: DATE;WBAN;MAX_DAILY_TEMP_IN_WBAN;TIME_OF_MEASUREMENT

$old_key = '';
$wban_max = '';
$key_max = '';
$max_hour = '';
while(<stdin>) {

	@chunks = split(';', $_);
	

	if (($old_key eq $chunks[0]) && ($wban_max eq $chunks[1])){
		if($chunks[2] > $key_max) {
			$key_max = $chunks[2];
			$max_hour = $chunks[3];
		}
	} else {
		if ($old_key ne '') {
			print $old_key . ";" . $wban_max . ";" . $key_max . ";" . $max_hour;
		}
		$old_key = $chunks[0];
		$wban_max = $chunks[1];
		$key_max = $chunks[2];		
		$max_hour = $chunks[3];
	}
}

if ($old_key ne "Date") {
	print $old_key . ";" . $wban_max . ";" . $key_max . ";" . $max_hour;
}

