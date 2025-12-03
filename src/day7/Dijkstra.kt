package day7

import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.*

private val NODE_DISTANCE_COMPARATOR = Comparator<Node> { o1, o2 -> Integer.compare(o1!!.distance, o2!!.distance) }


class MultiQueuePrioriryScheduler<E>(
    val comparator: Comparator<E>,
    val numberOfQueues: Int
) {

    private val queues = Array(numberOfQueues) {
        PriorityQueue(comparator)
    }
    private val locks = Array(numberOfQueues) {
        ReentrantLock()
    }

    fun poll(): E? {
        while (true) {
            val randomIndex1 = getRandomInt()
            val randomIndex2 = getRandomInt()

            val (index1, index2) = if (randomIndex1 < randomIndex2)
                randomIndex1 to randomIndex2
            else
                randomIndex2 to randomIndex1

            if (index1 == index2)
                continue

            if (!lockQueues(index1, index2))
                continue

            val peek1 = queues[index1].peek()
            val peek2 = queues[index2].peek()

            if (peek1 == null) {
                if (peek2 == null) {
                    unlockQueues(index1, index2)
                    return null
                }
                queues[index2].poll()
                unlockQueues(index1, index2)
                return peek2
            }

            if (peek2 == null) {
                queues[index1].poll()
                unlockQueues(index1, index2)
                return peek1
            }

            if (comparator.compare(peek1, peek2) <= 0) {
                queues[index1].poll()
                unlockQueues(index1, index2)
                return peek1
            } else {
                queues[index2].poll()
                unlockQueues(index1, index2)
                return peek2
            }
        }
    }

    fun add(element: E) {
        while (true) {
            val index = getRandomInt()

            if(!locks[index].tryLock())
                continue

            queues[index].add(element)
            locks[index].unlock()
            return
        }
    }

    private fun lockQueues(index1: Int, index2: Int): Boolean {
        if (locks[index1].tryLock()) {
            if (locks[index2].tryLock()) {
                return true
            }
            locks[index1].unlock()
            return false
        }

        return false
    }

    private fun unlockQueues(index1: Int, index2: Int) {
        locks[index2].unlock()
        locks[index1].unlock()
    }

    private fun getRandomInt(): Int = ThreadLocalRandom.current().nextInt(numberOfQueues)
}

// Returns `Integer.MAX_VALUE` if a path has not been found.
fun shortestPathParallel(start: Node) {
    val workers = Runtime.getRuntime().availableProcessors()
    // The distance to the start node is `0`
    start.distance = 0
    // Create a priority (by distance) queue and add the start node into it
    val q = MultiQueuePrioriryScheduler(
        comparator = NODE_DISTANCE_COMPARATOR,
        workers * 3
    )

    val activeNodes = AtomicInteger()
    q.add(start)
    activeNodes.incrementAndGet()

    // Run worker threads and wait until the total work is done
    val onFinish = Phaser(workers + 1) // `arrive()` should be invoked at the end by each worker
    repeat(workers) {
        thread {
            while (true) {
                val node = q.poll()
                if (activeNodes.get() == 0 && node == null) {
                    break
                }
                if (node == null)
                    continue

                node.outgoingEdges.forEach { edge ->
                    while (true) {
                        val currentDistance = edge.to.distance
                        val newDistance = node.distance + edge.weight

                        if (newDistance >= currentDistance)
                            break

                        if (edge.to.casDistance(currentDistance, newDistance)) {
                            activeNodes.incrementAndGet()
                            q.add(edge.to)
                            break
                        }
                    }
                }
                activeNodes.decrementAndGet()
            }
            onFinish.arrive()
        }
    }
    onFinish.arriveAndAwaitAdvance()
}