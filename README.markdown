# Hadoop Pig user-defined functions

This project wraps up a few custom functions I have found useful in using pig. 

These function support pig 0.3 at present. I have not tested them in newer versions yet.

I haven't included a build script at this time.  


## CSV file Loader

Although PigStorage supports comma delimited files out of the box, it does not properly 
parse fields that contain commas. These fields are wrapped in double quotes like:
"easy as 1,2,3". 

The loader is a quick and dirty modification of PigStorage, but it only supports file input.


## Date Parsers and formatters

Two eval functions to parse dates and convert them to a number of seconds since the epoch and back.
Currently only supports one date format.





