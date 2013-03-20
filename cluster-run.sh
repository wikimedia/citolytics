#!/bin/bash
#        __
#        \ \
#   _   _ \ \  ______
#  | | | | > \(  __  )
#  | |_| |/ ^ \| || |
#  | ._,_/_/ \_\_||_|
#  | |
#  |_|
# 
# ----------------------------------------------------------------------------
# "THE BEER-WARE LICENSE" (Revision 42):
# <rob ∂ CLABS dot CC> wrote this file. As long as you retain this notice you
# can do whatever you want with this stuff. If we meet some day, and you think
# this stuff is worth it, you can buy me a beer in return.
# ----------------------------------------------------------------------------

DIR=`pwd`

DATASET=path/to/dataset
OUTPUT=path/to/output

MLP=/path/to/this-project/target/mlp.jar
MODEL=/path/to/stanford/postagger/model/file

STRATOSPHERE=/path/to/stratosphere
CLIENT=$STRATOSPHERE/bin/pact-client.sh
LOGFILE=$STRATOSPHERE/log/nephele-rob-jobmanager-<your machine name>.log


ALPHA="1"
BETA="1"
GAMMA="0.5"
THRESHOLD="0.8"


# ----------------------------------------
#  *** This is where the magic happens ***
#
#      IF YOUR DON'T KNOW WHAT YOU DO
#      BETTER DON'T PASS THIS COMMENT!
# ----------------------------------------

function colorcodes () {
	# special chars
	esc=""
	blackf="${esc}[30m";	redf="${esc}[31m";	greenf="${esc}[32m"
	yellowf="${esc}[33m";	bluef="${esc}[34m";	purplef="${esc}[35m"
	cyanf="${esc}[36m";		whitef="${esc}[37m"
	blackb="${esc}[40m";	redb="${esc}[41m";	greenb="${esc}[42m"
	yellowb="${esc}[43m";	blueb="${esc}[44m";	purpleb="${esc}[45m"
	cyanb="${esc}[46m";		whiteb="${esc}[47m"
	boldon="${esc}[1m";		boldoff="${esc}[22m"
	italicson="${esc}[3m";	italicsoff="${esc}[23m"
	ulon="${esc}[4m";		uloff="${esc}[24m"
	invon="${esc}[7m";		invoff="${esc}[27m"
	reset="${esc}[0m"
}

function drawlogo () {
	echo "${whitef}"
	echo "        __"
	echo "        \ \\"
	echo "   _   _ \ \  ______"
	echo "  | | | | > \(  __  )"
	echo "  | |_| |/ ^ \| || |"
	echo "  | ._,_/_/ \_\_||_|"
	echo "  | |"
	echo "  |_|"
	echo "${reset}"
}

function log () {
	if test $# -gt 1; then
		case $1 in
			"EXIT")
					echo "${redb}${whitef}$2${reset}" 1>&2;;
			"INFO")
					echo "${boldon}${yellowf}$2 ...${boldoff}${reset}"
		esac
	else
		echo "$1"
	fi
}

function die () {
	echo ""
	log EXIT "${1:-"Have a nice day ...  (^_^)/\""}";
	exit 1;
}

clear
colorcodes
drawlogo

log INFO "sending mlp job to stratosphere"
($CLIENT run -j $MLP -a file://$DATASET file://$OUTPUT $MODEL $ALPHA $BETA $GAMMA $THRESHOLD & tail -f -n 0 $LOGFILE) || die