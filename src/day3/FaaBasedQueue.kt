package day3

import day2.*
import java.util.concurrent.atomic.*

class FAABasedQueue<E> : Queue<E> {
    val deqIdx = AtomicLong(0)
    val enqIdx = AtomicLong(0)
    val tailSegment: AtomicReference<Segment>
    val headSegment: AtomicReference<Segment>

    init {
        val dummy = Segment(0)
        headSegment = AtomicReference(dummy)
        tailSegment = AtomicReference(dummy)
    }

    override fun enqueue(element: E) {
        while (true) {
            val currTail = tailSegment.get()
            val index = enqIdx.getAndIncrement()
            val segment = findSegment(currTail, index)
            moveTailForward(currTail, segment)
            val pos = (index % SEGMENT_SIZE).toInt()
            if (segment.cells.compareAndSet(pos, null, element))
                return
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun dequeue(): E? {
        while (true) {
            if (!shouldTryToDequeue())
                return null
            val currHead = headSegment.get()
            val index = deqIdx.getAndIncrement()
            val segment = findSegment(currHead, index)
            moveHeadForward(currHead, segment)
            val pos = (index % SEGMENT_SIZE).toInt()

            if (segment.cells.compareAndSet(pos, null, TOMBSTONE))
                continue

            return segment.cells.get(pos) as E
        }
    }

    private fun shouldTryToDequeue(): Boolean {
        while (true) {
            val currDeqIdx = deqIdx.get()
            val currEnqIdx = enqIdx.get()
            if (currDeqIdx != deqIdx.get())
                continue
            return currDeqIdx < currEnqIdx
        }
    }

    private fun findSegment(start: Segment, index: Long): Segment {
        val targetIndex = index / SEGMENT_SIZE
        var currSegment = start

        while (currSegment.id < targetIndex) {
            var next = currSegment.next.get()
            if (next == null) {
                val newSegment = Segment(currSegment.id + 1)
                next = if (currSegment.next.compareAndSet(null, newSegment)) {
                    newSegment
                } else {
                    currSegment.next.get()
                }
            }
            currSegment = next!!
        }

        return currSegment
    }

    private fun moveHeadForward(currHead: Segment, segment: Segment) {
        if (segment.id > currHead.id) {
            headSegment.compareAndSet(currHead, segment)
        }
    }

    private fun moveTailForward(currTail: Segment, segment: Segment) {
        if (segment.id > currTail.id) {
            tailSegment.compareAndSet(currTail, segment)
        }
    }

    companion object {
        private val TOMBSTONE = Any()
    }
}

class Segment(val id: Long) {
    val next = AtomicReference<Segment?>(null)
    val cells = AtomicReferenceArray<Any?>(SEGMENT_SIZE)
}

// DO NOT CHANGE THIS CONSTANT
private const val SEGMENT_SIZE = 2