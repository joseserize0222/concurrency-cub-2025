@file:Suppress("UNCHECKED_CAST")

package day6

import java.util.concurrent.atomic.*

class ConcurrentHashTable<K : Any, V : Any>(initialCapacity: Int) {
    private val table = AtomicReference(Table<K, V>(initialCapacity))

    fun put(key: K, value: V): V? {
        while (true) {
            // Try to insert the key/value pair.
            val currentTable  = table.get()
            val putResult = currentTable.put(key, value)
            if (putResult === NEEDS_REHASH) {
                // The current table is too small to insert a new key.
                // Create a new table of x2 capacity,
                // copy all elements to it,
                // and restart the current operation.
                resize(currentTable)
            } else {
                // The operation has been successfully performed,
                // return the previous value associated with the key.
                return putResult as V?
            }
        }
    }

    fun get(key: K): V? {
        return table.get().get(key)
    }

    fun remove(key: K): V? {
        return table.get().remove(key)
    }

    fun resize(currentTable: Table<K, V>) {

        val newTable = Table<K, V>(currentTable.capacity * 2)

        if (currentTable.next.compareAndSet(null, newTable)) {
            // we created the next table
            for (i in 0 until currentTable.capacity) {
                currentTable.helpCopySlot(i)
            }
            table.compareAndSet(currentTable, newTable)
        } else {
            // we will help to progress
            for (i in 0 until currentTable.capacity) {
                currentTable.helpCopySlot(i)
            }
            table.compareAndSet(currentTable, table.get().next.get())
        }
    }


    class Table<K : Any, V : Any>(val capacity: Int) {
        val keys = AtomicReferenceArray<Any?>(capacity)
        val values = AtomicReferenceArray<Any?>(capacity)
        val next = AtomicReference<Table<K, V>>(null)

        fun put(key: K, value: V): Any? {
            var index = getIndex(key)
            val start = index

            retry@ while (true) {

                // table already set to be copied
                if (next.get() != null) {
                    helpCopySlot(index)
                    // return NEEDS_REHASH
                }

                when (val k = keys.get(index)) {
                    is KeyValue -> { // put in progress, help the put
                        helpPutEntry(index, k)
                        continue // retry
                    }

                    is PrimeKey -> { // slot is guaranteed to be copied
                        if (k.key == key) {
                            return next.get().put(key, value)
                        }
                    }

                    null -> { // empty slot, try to occupy
                        val keyValue = KeyValue(key, value)
                        if (keys.compareAndSet(index, null, keyValue)) {
                            // if we succeeded then this update is guaranteed to be done by this thread or the rest that helped this update
                            helpPutEntry(index, keyValue)
                            return null // there was nothing here
                        } else {
                            continue // someone modified the key, retry
                        }
                    }

                    key -> { // update the value
                        while (true) {
                            val oldVal = values.get(index)

                            // slot is being copied
                            if (oldVal is PrimeValue) {
                                helpCopySlot(index)
                                continue@retry
                            }

                            // oldVal can be tombstone or normal value, CAS to substitute
                            if (values.compareAndSet(index, oldVal, value)) {
                                // return the oldVal
                                return if (oldVal === TOMBSTONE) // element was deleted previously
                                    null
                                else
                                    oldVal as V?
                            }
                            // CAS failed, other put won the slot
                        }
                    }
                }

                // linear prob
                index = (index + 1) % capacity

                // the entire table was visited, we need to rehash
                if (index == start) {
                    return NEEDS_REHASH
                }
            }
        }

        fun get(key: K): V? {
            var index = getIndex(key)
            val start = index

            while (true) {
                val nt = next.get()

                // table in resize, help the resize
                if (nt != null) {
                    helpCopySlot(index)
                }

                when (val k = keys.get(index)) {
                    is KeyValue -> { // put in progress, help the put
                        helpPutEntry(index, k)
                        continue // retry
                    }

                    is PrimeKey -> { // If we see k marked as prime, then we are guaranteed that was transferred to the next table
                        if (k.key == key) {
                            // the key matches, delegate to the next table
                            return next.get().get(key)
                        }
                    }

                    null -> return null // empty slot => no element here

                    key -> {  // found a match
                        return when (val v = values.get(index))  {
                            TOMBSTONE -> {
                                null
                            }

                            is PrimeValue -> {
                                v.value as V?
                            }

                            else -> {
                                v as V?
                            }
                        }
                    }
                }

                // Probing
                index = (index + 1) % capacity

                // We tried all we could
                if (index == start) return null
            }
        }

        fun remove(key: K): V? {
            var index = getIndex(key)
            val start = index

            while (true) {

                val nt = next.get()

                if (nt != null) {
                    helpCopySlot(index)
                }

                when (val k = keys.get(index)) {
                    is KeyValue -> { // put in progress, help the put
                        helpPutEntry(index, k)
                        continue // retry
                    }

                    is PrimeKey -> { // if we see k prime then the operation was done physically on the next table
                        if (k.key == key) {
                            return next.get().remove(key)
                        }
                    }

                    null -> return null // the element does not exists

                    key -> { // if k is not prime
                        while(true) {
                            when (val oldVal = values.get(index)) {
                                is PrimeValue -> { // rehash in progress
                                    helpCopySlot(index) // we help migrating
                                    return next.get().remove(key) // delegate
                                }
                                TOMBSTONE -> {
                                    return null // element was already deleted
                                }

                                else -> { // try to delete
                                    if (values.compareAndSet(index, oldVal, TOMBSTONE)) {
                                        @Suppress("UNCHECKED_CAST")
                                        return oldVal as V?
                                    }
                                    // CAS failed someone modified the value, retry
                                }
                            }
                        }
                    }
                }

                // probing
                index = (index + 1) % capacity
                if (index == start) return null
            }
        }

        fun getIndex(key: K): Int {
            return (key.hashCode() and Int.MAX_VALUE) % capacity
        }

        fun helpCopySlot(i: Int) {
            val nextTable = next.get() ?: return

            while (true) {
                when (val k = keys.get(i)) {
                    null -> return // nothing to copy

                    is PrimeKey -> return // copy was done

                    is KeyValue -> { // put in progress, help the put
                        helpPutEntry(i, k)
                        continue
                    }

                    else -> {
                        val indexInNewTable = nextTable.getIndex(k as K)

                        when (val v = values.get(i)) {
                            null, TOMBSTONE -> return // element is deleted do nothing

                            is PrimeValue -> {
                                // object is being copied, help
                                val keyValue = KeyValue(k, v.value)

                                // install the put in the new table
                                // installment is guaranteed to be done by this or other threads, lazy put
                                // readers and writers of the other table need to finish this put before proceeding
                                nextTable.keys.compareAndSet(indexInNewTable, null, keyValue)

                                // transference was successful
                                keys.set(i, PrimeKey(k))
                                return
                            }

                            else -> {
                                // v here can be normal v only
                                // try to install in the new table

                                val keyValue = KeyValue(k, v)

                                if (!values.compareAndSet(i, v, PrimeValue(v)))
                                // if we don't start the transaction the value was modified, start again
                                    continue

                                // install the put in the new table
                                nextTable.keys.compareAndSet(indexInNewTable, null, keyValue)

                                // transference was successful
                                keys.set(i, PrimeKey(k))
                                return
                            }
                        }
                    }
                }
            }
        }

        fun helpPutEntry(index: Int, kv: KeyValue) {
            values.compareAndSet(index, null, kv.value)
            keys.compareAndSet(index, kv, kv.key)
        }
    }
}

private val NEEDS_REHASH = Any()

private val TOMBSTONE = Any()

class KeyValue(
    val key: Any?,
    val value: Any?
)

class PrimeKey(
    val key: Any?
)

class PrimeValue(
    val value: Any?
)