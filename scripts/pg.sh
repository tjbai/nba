#!/bin/bash

ls pickles | cut -d "_" -f 1 | sort | uniq -c
