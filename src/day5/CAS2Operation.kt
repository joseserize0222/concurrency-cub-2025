package day5

import java.util.concurrent.atomic.AtomicReference

data class CAS2Descriptor(
    val expectedA: Any?,
    val expectedB: Any?,
    val updateA: Any?,
    val updateB: Any?,
) {
    val status: AtomicReference<Status> = AtomicReference(Status.UNDECIDED)
}

class CAS2Operation(
    private val indexA: Int,
    private val indexB: Int,
    private val getArrayElement: (index: Int) -> Any?,
    private val compareAndSetArrayElement: (index: Int, Any?, Any?) -> Boolean,
    val descriptor: CAS2Descriptor
): DescriptorOperation {
    private val expectedA get() = descriptor.expectedA
    private val expectedB get() = descriptor.expectedB
    private val updateA get() = descriptor.updateA
    private val updateB get() = descriptor.updateB
    private val status get() = descriptor.status

    val operationResult
        get() = status.get() == Status.SUCCESS

    override fun run(): Boolean {
        if (status.get() != Status.UNDECIDED) {
            apply()
            return operationResult
        }

        if (!install()) {
            status.compareAndSet(Status.UNDECIDED, Status.FAILURE)
            return operationResult
        }

        if (!dcss()) {
            status.compareAndSet(Status.UNDECIDED, Status.FAILURE)
            apply()
            return operationResult
        }

        status.compareAndSet(Status.UNDECIDED, Status.SUCCESS)
        apply()
        return operationResult
    }

    override fun install(): Boolean {
        while (true) {
            when (val value = getArrayElement(indexA)) {
                this -> return true
                expectedA -> {
                    if (compareAndSetArrayElement(indexA, expectedA, this)) {
                        return true
                    }
                }
                is DescriptorOperation -> value.run()
                else -> return false
            }
        }
    }

    override fun apply() {
        applyOnIndex(indexA, expectedA, updateA)
        applyOnIndex(indexB, expectedB, updateB)
    }

    override fun readIndex(index: Int): Any? =  if (operationResult) {
            if (index == indexA) updateA else updateB
        } else {
            if (index == indexA) expectedA else expectedB
        }

    private fun dcss(): Boolean {
        while (true) {
            when (val value = getArrayElement(indexB)) {
                this -> return true
                expectedB -> {
                    return if (status.get() != Status.UNDECIDED) {
                        return status.get() == Status.SUCCESS
                    } else DCSSOperation(
                        cas2Status = status,
                        getArrayElement = {
                            getArrayElement(indexB)
                        },
                        compareAndSetArrayElement = { expected: Any?, update: Any? ->
                            compareAndSetArrayElement(indexB, expected, update)
                        },
                        descriptor = DCSSDescriptor(
                            expected = expectedB,
                            expectedCAS2Status = Status.UNDECIDED,
                            update = this
                        )
                    ).run()
                }
                is DescriptorOperation -> value.run()
                else -> return false
            }
        }
    }

    private fun applyOnIndex(index: Int, expected: Any?, update: Any?) {
        while (true) {
            if (getArrayElement(index) !== this)
                return
            val newValue = if (status.get() == Status.SUCCESS) update else expected
            if (compareAndSetArrayElement(index, this, newValue)) {
                return
            }
        }
    }
}
