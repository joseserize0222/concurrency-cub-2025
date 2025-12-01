@file:Suppress("FoldInitializerAndIfToElvis", "DuplicatedCode")

package day4

import day4.*
import java.util.concurrent.atomic.*

class MSQueueWithLinearTimeRemove<E> : QueueWithRemove<E> {
    private val head: AtomicReference<Node>
    private val tail: AtomicReference<Node>

    init {
        val dummy = Node(null)
        head = AtomicReference(dummy)
        tail = AtomicReference(dummy)
    }

    override fun enqueue(element: E) {
        val node = Node(element)
        while (true) {
            val currTail = tail.get()

            if (currTail.next.compareAndSet(null, node)) {
                tail.compareAndSet(currTail, node)
                if (currTail.extractedOrRemoved)
                    currTail.remove()
                return
            } else {
                tail.compareAndSet(currTail, currTail.next.get())
                if (currTail.extractedOrRemoved)
                    currTail.remove()
            }
        }
    }

    override fun dequeue(): E? {
        while (true) {
            val currHead = head.get()
            val currHeadNext = currHead.next.get() ?: return null
            val value = currHeadNext.element

            if (head.compareAndSet(currHead, currHeadNext)) {
                if (currHeadNext.markExtractedOrRemoved()) {
                    return value
                }
            }
        }
    }

    override fun remove(element: E): Boolean {
        // Traverse the linked list, searching the specified
        // element. Try to remove the corresponding node if found.
        // DO NOT CHANGE THIS CODE.
        var node = head.get()
        while (true) {
            val next = node.next.get()
            if (next == null) return false
            node = next
            if (node.element == element && node.remove()) return true
        }
    }

    /**
     * This is an internal function for tests.
     * DO NOT CHANGE THIS CODE.
     */
    override fun validate() {
        check(tail.get().next.get() == null) {
            "tail.next must be null"
        }
        var node = head.get()
        // Traverse the linked list
        while (true) {
            if (node !== head.get() && node !== tail.get()) {
                check(!node.extractedOrRemoved) {
                    "Removed node with element ${node.element} found in the middle of this queue"
                }
            }
            node = node.next.get() ?: break
        }
    }

    // TODO: Node is an inner class for accessing `head` in `remove()`
    private inner class Node(
        var element: E?
    ) {
        val next = AtomicReference<Node?>(null)

        /**
         * TODO: Both [dequeue] and [remove] should mark
         * TODO: nodes as "extracted or removed".
         */
        private val _extractedOrRemoved = AtomicBoolean(false)
        val extractedOrRemoved
            get() =
                _extractedOrRemoved.get()

        fun markExtractedOrRemoved(): Boolean =
            _extractedOrRemoved.compareAndSet(false, true)

        /**
         * Removes this node from the queue structure.
         * Returns `true` if this node was successfully
         * removed, or `false` if it has already been
         * removed by [remove] or extracted by [dequeue].
         */
        fun remove(): Boolean {
            // ---- logical removal
            val removed = markExtractedOrRemoved()

            // ---- physical removal
            val currPrev = findPrev() ?: return removed // do not remove the head
            val currNext = next.get()  ?: return removed // do not remove the tail
            currPrev.next.compareAndSet(this, currNext)
            if (currNext.extractedOrRemoved) {
                currNext.remove()
            }
            return removed
        }

        private fun findPrev(): Node? {
            var currNode = head.get()
            while (true) {
                if (currNode == null)
                    return null
                if (currNode.next.get() == this) {
                    return currNode
                }
                currNode = currNode.next.get()
            }
        }
    }
}
