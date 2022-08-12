lines = LOAD 'InputForWC.txt' USING TextLoader  AS (line:chararray);
token_data = FOREACH lines GENERATE TOKENIZE(line,'\t \\s+');
flat= FOREACH token_data GENERATE flatten($0);
grouped = GROUP flat BY $0;
word_count = FOREACH grouped GENERATE group, COUNT($1);
STORE word_count INTO 'WordCount' USING PigStorage(',');