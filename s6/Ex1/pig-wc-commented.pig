//Fetch all lines and store them in Tuples
lines  = LOAD 'InputForWC.txt' USING TextLoader AS (line:chararray);

//Tokenize Lines
tokenized_data = FOREACH lines GENERATE TOKENIZE(line,'\t');

//Flatten
flatten_data = FOREACH tokenized_data GENERATE flatten($0);

//Tokenize again for last row. 
flatten_data_l2 = FOREACH flatten_data GENERATE TOKENIZE($0,' ');

//Flatten Second Time
flatten_data_l3 = FOREACH flatten_data_l2 GENERATE flatten($0);

//Group
 grouped_data = GROUP flatten_data_l3 BY $0;

//Extract Count
word_count = FOREACH grouped_data Generate $0, COUNT($1);

//Store...
STORE word_count INTO 'WordCount' USING PigStorage(',');



