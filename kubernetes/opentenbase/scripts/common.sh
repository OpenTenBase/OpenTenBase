#!/usr/bin/env bash

# black="\e[0;30m"
# red="\e[0;31m"
# green="\e[0;32m"
# yellow="\e[0;33m"
# blue="\e[0;34m"
# purple="\e[0;35m"
# cyan="\e[0;36m"
# white="\e[0;37m"
# orange="\e[0;91m"

# Methods and functions to be used in all other scripts.
: ${LOGGING_SHOW_COLORS:="true"}
: ${LOGGING_INFO_PREFIX_COLOR:="\e[0;32m"}
: ${LOGGING_INFO_PREFIX:=":INFO"}
: ${LOGGING_ERROR_PREFIX_COLOR:="\e[0;31m"}
: ${LOGGING_ERROR_PREFIX:=":ERROR"}
: ${LOGGING_WARNING_PREFIX_COLOR:="\e[0;33m"}
: ${LOGGING_WARNING_PREFIX:=":WARNING"}
: ${LOGGING_ARCHIVE_PREFIX_COLOR:="\e[0;34m"}
: ${LOGGING_ARCHIVE_PREFIX:=":INFO"}
: ${LOGGING_SCRIPT_PREFIX:=":INFO"}
: ${LOGGING_SCRIPT_PREFIX_COLOR:="\e[0;36m"}
: ${LOGGING_SCRIPT_TEXT_COLOR:="\e[0;35m"}

# TODO: Apply common format for stackdriver.
logging_core_print() {
    local LOG_TYPE="$1"
    local RESET_COLOR="\e[0m"
    # remove the first argument.
    shift

    local LOG_TYPE_PREFIX_ENV_NAME="LOGGING_${LOG_TYPE}_PREFIX"
    LOG_TYPE_PREFIX="${!LOG_TYPE_PREFIX_ENV_NAME}"

    if [ "$LOGGING_SHOW_COLORS" != "true" ]; then
        echo "${LOGGING_PREFIX}${LOG_TYPE_PREFIX} " "$@"
    else
        local LOG_PREFIX_COLOR_ENV_NAME="LOGGING_${LOG_TYPE}_PREFIX_COLOR"
        local LOG_TEXT_COLOR_ENV_NAME="LOGGING_${LOG_TYPE}_TEXT_COLOR"
        LOG_PREFIX_COLOR="${!LOG_PREFIX_COLOR_ENV_NAME}"
        LOG_TEXT_COLOR="${!LOG_TEXT_COLOR_ENV_NAME}"

        if [ -z "${LOG_TEXT_COLOR}" ]; then LOG_TEXT_COLOR="${RESET_COLOR}"; fi

        echo -e "${LOG_PREFIX_COLOR}${LOGGING_PREFIX}${LOG_TYPE_PREFIX}${LOG_TEXT_COLOR}" "$@" "${RESET_COLOR}"
    fi
}

# log a line with the logging prefex.
function log:info() {
    logging_core_print "INFO" "$@"
}

function log:error() {
    logging_core_print "ERROR" "$@"
}

function log:warning() {
    logging_core_print "WARNING" "$@"
}

function log:script() {
    logging_core_print "SCRIPT" "$@"
}

function log:archive() {
    logging_core_print "ARCHIVE" "$@"
}

function print_bash_error_stack() {
    for ((i = 1; i < ${#FUNCNAME[@]} - 1; i++)); do
        local FPATH
        FPATH="$(realpath "${BASH_SOURCE[$i + 1]}")"
        log:error "$i: ${FPATH}:${BASH_LINENO[$i]} @ ${FUNCNAME[$i]}"
    done
}

function assert() {
    if [ "$1" -ne 0 ]; then
        log:error "$2"
        print_bash_error_stack
        return "$1"
    fi
}

function assert_warn() {
    if [ "$1" -ne 0 ]; then
        log:warning "$2"
    fi
}
