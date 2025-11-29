@file:Suppress("DuplicatedCode")

package day1

import day1.Bank.Companion.MAX_AMOUNT
import java.util.concurrent.locks.*

class FineGrainedBank(accountsNumber: Int) : Bank {
    private val accounts: Array<Account> = Array(accountsNumber) { Account() }

    override fun getAmount(id: Int): Long {
        val account = accounts[id]
        return criticalSection(account.lock) {
             account.amount
        }
    }

    override fun deposit(id: Int, amount: Long): Long {
        require(amount > 0) { "Invalid amount: $amount" }
        val account = accounts[id]
        return criticalSection(account.lock) {
            check(!(amount > MAX_AMOUNT || account.amount + amount > MAX_AMOUNT)) { "Overflow" }
            account.amount += amount
            account.amount
        }
    }

    override fun withdraw(id: Int, amount: Long): Long {
        require(amount > 0) { "Invalid amount: $amount" }
        val account = accounts[id]
        return criticalSection(account.lock) {
            check(account.amount - amount >= 0) { "Underflow" }
            account.amount -= amount
            account.amount
        }
    }

    override fun transfer(fromId: Int, toId: Int, amount: Long) {
        require(amount > 0) { "Invalid amount: $amount" }
        require(fromId != toId) { "fromId == toId" }
        val firstId = minOf(fromId, toId)
        val secondId = maxOf(fromId, toId)

        val firstAccount = accounts[firstId]
        val secondAccount = accounts[secondId]

        criticalSection(firstAccount.lock) {
            criticalSection(secondAccount.lock) {
                val from = if (fromId == firstId) firstAccount else secondAccount
                val to = if (toId == secondId) secondAccount else firstAccount
                check(amount <= from.amount) { "Underflow" }
                check(!(amount > MAX_AMOUNT || to.amount + amount > MAX_AMOUNT)) { "Overflow" }
                from.amount -= amount
                to.amount += amount
            }
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

        val lock = ReentrantLock()
    }
}