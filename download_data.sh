#!/bin/bash

export PATH=$PATH:~/.local/bin

kaggle datasets download -d sahityasahu/amazon-review-dataset --unzip -p /staging/taffet/
