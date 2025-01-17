===============
idset_create(3)
===============


SYNOPSIS
========

::

   #include <flux/idset.h>

::

   struct idset *idset_create (size_t slots, int flags);

::

   void idset_destroy (struct idset *idset);

::

   struct idset *idset_copy (const struct idset *idset);

::

   int idset_set (struct idset *idset, unsigned int id);

::

   int idset_range_set (struct idset *idset,
                        unsigned int lo, unsigned int hi);

::

   int idset_clear (struct idset *idset, unsigned int id);

::

   int idset_range_clear (struct idset *idset,
                          unsigned int lo, unsigned int hi)

::

   bool idset_test (const struct idset *idset, unsigned int id);

::

   unsigned int idset_first (const struct idset *idset);

::

   unsigned int idset_next (const struct idset *idset, unsigned int prev);

::

   unsigned int idset_last (const struct idset *idset)

::

   size_t idset_count (const struct idset *idset);

::

   bool idset_equal (const struct idset *set1, const struct idset *set2);


USAGE
=====

cc [flags] files -lflux-idset [libraries]


DESCRIPTION
===========

An idset is a set of numerically sorted, non-negative integers.
It is internally represented as a van Embde Boas (or vEB) tree.
Functionally it behaves like a bitmap, and has space efficiency
comparable to a bitmap, but performs operations (insert, delete,
lookup, findNext, findPrevious) in O(log(m)) time, where pow (2,m)
is the number of slots in the idset.

``idset_create()`` creates an idset. *slots* specifies the highest
numbered *id* it can hold, plus one. The size is fixed unless
*flags* specify otherwise (see FLAGS below).

``idset_destroy()`` destroys an idset.

``idset_copy()`` copies an idset.

``idset_set()`` and ``idset_clear()`` set or clear *id*.

``idset_range_set()`` and ``idset_range_clear()`` set or clear an inclusive
range of ids, from *lo* to *hi*.

``idset_test()`` returns true if *id* is set, false if not.

``idset_first()`` and ``idset_next()`` can be used to iterate over ids
in the set, returning IDSET_INVALID_ID at the end. ``idset_last()``
returns the last (highest) id, or IDSET_INVALID_ID if the set is
empty.

``idset_count()`` returns the number of ids in the set.

``idset_equal()`` returns true if the two idset objects *set1* and *set2*
are equal sets, i.e. the sets contain the same set of integers.


FLAGS
=====

IDSET_FLAG_AUTOGROW
   Valid for ``idset_create()`` only. If set, the idset will grow to
   accommodate any id inserted into it. The internal vEB tree is doubled
   in size until until the new id can be inserted. Resizing is a costly
   operation that requires all ids in the old tree to be inserted into
   the new one.


RETURN VALUE
============

``idset_copy()`` returns an idset on success which must be freed with
``idset_destroy()``. On error, NULL is returned with errno set.

``idset_first()``, ``idset_next()``, and ``idset_last()`` return an id,
or IDSET_INVALID_ID if no id is available.

``idset_equal()`` returns true if *set1* and *set2* are equal sets,
or false if they are not equal, or either argument is *NULL*.

Other functions return 0 on success, or -1 on error with errno set.


ERRORS
======

EINVAL
   One or more arguments were invalid.

ENOMEM
   Out of memory.


RESOURCES
=========

Github: http://github.com/flux-framework


SEE ALSO
========

idset_encode(3), idset_add(3)

`RFC 22: Idset String Representation <https://github.com/flux-framework/rfc/blob/master/spec_22.rst>`__
