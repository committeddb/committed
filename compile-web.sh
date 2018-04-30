#!/bin/bash

cd webapp
npm run-script build

cd ..
statik -src webapp/build/