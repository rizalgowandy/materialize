# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Source definitions
# ------------------

# Define t0 source
define
DefSource name=t0
  - c0: bigint
  - c1: bigint
----
Source defined as t0


## Support for wrap- variants
## --------------------------


# Rewrite possible, one column
apply pipeline=flatmap_to_map
FlatMap wrap1(42)
  Get t0
----
Map (42)
  Get t0

# Rewrite possible, two columns
apply pipeline=flatmap_to_map
FlatMap wrap2(0, 7)
  Get t0
----
Map (0, 7)
  Get t0

# Rewrite possible, three columns
apply pipeline=flatmap_to_map
FlatMap wrap3(17, 42, 15)
  Get t0
----
Map (17, 42, 15)
  Get t0

# Rewrite possible, bigger wrap width than input
apply pipeline=flatmap_to_map
FlatMap wrap3(17, 42)
  Get t0
----
Map (17, 42)
  Get t0

# Produces more than one row, must not rewrite these
apply pipeline=flatmap_to_map
FlatMap wrap1(0, 1, 2)
  Get t0
----
FlatMap wrap1(0, 1, 2)
  Get t0

# Produces more than one row, must not rewrite these
apply pipeline=flatmap_to_map
FlatMap wrap2(0, 1, 2, 3)
  Get t0
----
FlatMap wrap2(0, 1, 2, 3)
  Get t0

# Produces more than one row, must not rewrite these
apply pipeline=flatmap_to_map
FlatMap wrap3(0, 1, 2, 3)
  Get t0
----
FlatMap wrap3(0, 1, 2, 3)
  Get t0


## Support for unnest_~ calls
## --------------------------

# Rewrite possible for `unnset_array`
# Example SQL: select unnest(array[f1]) from t1 where f1 = 5;
apply pipeline=flatmap_to_map
FlatMap unnest_array({5})
  Get t0
----
Map (5)
  Get t0

# Rewrite possible for `unnset_list`
# Example SQL: select unnest(list[f1]) from t1 where f1 = 5;
apply pipeline=flatmap_to_map
FlatMap unnest_list([5])
  Get t0
----
Map (5)
  Get t0

# Rewrite not possible: unnest_array(-) argument is not resuced
apply pipeline=flatmap_to_map
FlatMap unnest_array(array[5])
  Get t0
----
FlatMap unnest_array(array[5])
  Get t0

# Rewrite not possible: unnest_list(-) argument is not resuced
apply pipeline=flatmap_to_map
FlatMap unnest_list(list[5])
  Get t0
----
FlatMap unnest_list(list[5])
  Get t0

# Rewrite not possible: unnest_array(-) argument is not a singleton
apply pipeline=flatmap_to_map
FlatMap unnest_list([5, 6])
  Get t0
----
FlatMap unnest_list([5, 6])
  Get t0

# Rewrite not possible: unnest_list(-) argument is not a singleton
apply pipeline=flatmap_to_map
FlatMap unnest_list(list[5])
  Get t0
----
FlatMap unnest_list(list[5])
  Get t0
