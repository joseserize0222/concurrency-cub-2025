package day5

interface DescriptorOperation {
    fun run(): Boolean

    fun install(): Boolean

    fun apply()

    fun readIndex(index: Int): Any?
}