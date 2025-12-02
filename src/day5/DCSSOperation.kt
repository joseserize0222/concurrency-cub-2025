package day5

import java.util.concurrent.atomic.AtomicReference


data class DCSSDescriptor(
    val expected: Any?,
    val expectedCAS2Status: Status,
    val update: CAS2Operation
) {
    val status: AtomicReference<Status> = AtomicReference(Status.UNDECIDED)
}

class DCSSOperation(
    private val cas2Status: AtomicReference<Status>,
    private val getArrayElement: () -> Any?,
    private val compareAndSetArrayElement: (Any?, Any?) -> Boolean,
    val descriptor: DCSSDescriptor,
): DescriptorOperation {

    private val status get() = descriptor.status

    private val expected get() = descriptor.expected

    private val expectedCAS2Status get() = descriptor.expectedCAS2Status

    private val update get() = descriptor.update

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

        if (cas2Status.get() != expectedCAS2Status) {
            status.compareAndSet(Status.UNDECIDED, Status.FAILURE)
            return operationResult
        }

        status.compareAndSet(Status.UNDECIDED, Status.SUCCESS)
        apply()
        return operationResult
    }

    override fun install(): Boolean {
        while (true) {
            when (val value = getArrayElement()) {
                this -> return true
                expected -> {
                    if (compareAndSetArrayElement(expected, this)) {
                        return true
                    }
                }
                is DescriptorOperation -> value.run()
                else -> return false
            }
        }
    }

    override fun apply() {
        while (true) {
            if (getArrayElement() !== this)
                return
            val newValue = if (status.get() == Status.SUCCESS) update else expected
            if (compareAndSetArrayElement(this, newValue)) {
                return
            }
        }
    }

    override fun readIndex(index: Int): Any? {
        return if (operationResult) {
            update.readIndex(index)
        } else {
            expected
        }
    }
}