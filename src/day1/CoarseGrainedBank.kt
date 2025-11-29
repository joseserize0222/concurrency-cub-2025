@file:Suppress("DuplicatedCode")

package day1

import day1.Bank.Companion.MAX_AMOUNT
import java.util.concurrent.locks.*

class CoarseGrainedBank(accountsNumber: Int) : Bank {
    private val accounts: Array<Account> = Array(accountsNumber) { Account() }

    private val globalLock = ReentrantLock()

    override fun getAmount(id: Int): Long {
        return criticalSection(globalLock) {
            accounts[id].amount
        }
    }

    override fun deposit(id: Int, amount: Long): Long {
        return criticalSection(globalLock) {
            require(amount > 0) { "Invalid amount: $amount" }
            val account = accounts[id]
            check(!(amount > MAX_AMOUNT || account.amount + amount > MAX_AMOUNT)) { "Overflow" }
            account.amount += amount
            account.amount
        }
    }

    override fun withdraw(id: Int, amount: Long): Long {
        return criticalSection(globalLock) {
            require(amount > 0) { "Invalid amount: $amount" }
            val account = accounts[id]
            check(account.amount - amount >= 0) { "Underflow" }
            account.amount -= amount
            account.amount
        }
    }

    override fun transfer(fromId: Int, toId: Int, amount: Long) {
        criticalSection(globalLock) {
            require(amount > 0) { "Invalid amount: $amount" }
            require(fromId != toId) { "fromIndex == toIndex" }
            val from = accounts[fromId]
            val to = accounts[toId]
            check(amount <= from.amount) { "Underflow" }
            check(!(amount > MAX_AMOUNT || to.amount + amount > MAX_AMOUNT)) { "Overflow" }
            from.amount -= amount
            to.amount += amount
        }
    }

    private fun<E> criticalSection(lock: ReentrantLock, block: () -> E): E {
        lock.lock()
        try {
            return block()
        } catch (e: Exception) {
            throw e
        } finally {
            lock.unlock()
        }
    }

    /**
     * Private account data structure.
     */
    class Account {
        /**
         * Amount of funds in this account.
         */
        var amount: Long = 0
    }
}