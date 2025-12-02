@file:Suppress("DuplicatedCode", "UNCHECKED_CAST")

package day5

import java.util.concurrent.atomic.*

// This implementation never stores `null` values.
class AtomicArrayWithCAS2<E : Any>(size: Int, initialValue: E) {
    private val array = AtomicReferenceArray<Any?>(size)

    private val getArrayElement = { index: Int -> array[index] }
    private val compareAndSetArrayElement =  { index: Int, expectedValue: Any?, updateValue: Any? ->
        array.compareAndSet(index, expectedValue, updateValue)
    }

    init {
        // Fill array with the initial value.
        for (i in 0 until size) {
            array[i] = initialValue
        }
    }

    fun get(index: Int): E? {
        val result = when (val value = array.get(index)) {
            is CAS2Operation -> {
                value.readIndex(index)
            }
            is DCSSOperation -> {
                value.readIndex(index)
            }
            else -> value
        }
        return result as E?
    }

    fun cas2(
        index1: Int, expected1: E?, update1: E?,
        index2: Int, expected2: E?, update2: E?
    ): Boolean {
        require(index1 != index2) { "The indices should be different" }

        val operation = if (index1 < index2) {
            val descriptor = CAS2Descriptor(
                expectedA = expected1,
                expectedB = expected2,
                updateA = update1,
                updateB = update2,
            )
            CAS2Operation(
                indexA = index1,
                indexB = index2,
                getArrayElement = getArrayElement,
                compareAndSetArrayElement = compareAndSetArrayElement,
                descriptor = descriptor
            )
        } else {
            val descriptor = CAS2Descriptor(
                expectedA = expected2,
                expectedB = expected1,
                updateA = update2,
                updateB = update1,
            )
            CAS2Operation(
                indexA = index2,
                indexB = index1,
                getArrayElement = getArrayElement,
                compareAndSetArrayElement = compareAndSetArrayElement,
                descriptor = descriptor
            )
        }

        return operation.run()
    }
}