/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.guard;

import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.logging.Log;

import static java.lang.System.currentTimeMillis;

public class TimeoutGuard implements Guard
{
    private final Log log;

    public TimeoutGuard( final Log log )
    {
        this.log = log;
    }

    @Override
    public void check( KernelStatement statement)
    {
        long now = currentTimeMillis();
        long transactionCompletionTime = getMaxTransactionCompletionTime( statement );
        if ( transactionCompletionTime < now )
        {
            final long overtime = now - transactionCompletionTime;
            log.warn( "Transaction timeout ( Overtime: " + overtime + " ms)." );
            throw new GuardTimeoutException( overtime );
        }
    }

    private long getMaxTransactionCompletionTime( KernelStatement statement )
    {
        return statement.getTransactionStartTime() + statement.getTransactionTimeout();
    }
}
