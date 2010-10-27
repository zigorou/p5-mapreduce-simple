use inc::Module::Install;
name 'MapReduce-Simple';
all_from 'lib/MapReduce/Simple.pm';

requires 'parent';
requires 'Carp';
requires 'Class::Accessor::Fast';
requires 'Data::Dumper';
requires 'Data::Util';
requires 'DBD::SQLite';
requires 'DBI';
requires 'Exporter';
requires 'File::Temp';
requires 'Parallel::ForkManager';
requires 'String::RewritePrefix';
requires 'Try::Tiny';
requires 'UNIVERSAL::require';

tests 't/*.t';
author_tests 'xt';

test_requires 'Test::More';
test_requires 'Test::Exception';
test_requires 'Test::LoadAllModules';

auto_set_repository;
auto_include;
WriteAll;
