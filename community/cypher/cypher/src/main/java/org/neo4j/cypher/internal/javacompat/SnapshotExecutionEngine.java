/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.javacompat;

import java.util.Map;

import org.neo4j.cypher.internal.CompatibilityFactory;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.pagecache.tracing.cursor.context.CursorContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.logging.LogProvider;

public class SnapshotExecutionEngine extends ExecutionEngine
{
    private final int maxQueryExecutionAttempts;

    SnapshotExecutionEngine( GraphDatabaseQueryService queryService, Config config, LogProvider logProvider,
            CompatibilityFactory compatibilityFactory )
    {
        super( queryService, logProvider, compatibilityFactory );
        this.maxQueryExecutionAttempts = config.get( GraphDatabaseSettings.snapshot_query_retries );
    }

    @Override
    public Result executeQuery( String query, Map<String,Object> parameters, TransactionalContext context )
            throws QueryExecutionKernelException
    {
        return executeWithRetries( query, parameters, context, super::executeQuery );
    }

    @Override
    public Result profileQuery( String query, Map<String,Object> parameters, TransactionalContext context )
            throws QueryExecutionKernelException
    {
        return executeWithRetries( query, parameters, context, super::profileQuery );
    }

    protected Result executeWithRetries( String query, Map<String,Object> parameters, TransactionalContext context,
            QueryExecutor executor ) throws QueryExecutionKernelException
    {
        CursorContext cursorContext = getCursorContext( context );
        EagerResult eagerResult;
        int attempt = 0;
        boolean dirtySnapshot;
        do
        {
            if ( attempt == maxQueryExecutionAttempts )
            {
                throw new QueryExecutionKernelException( new UnstableSnapshotException(
                        "Unable to get clean data snapshot for query '%s' after %d attempts.", query, attempt ) );
            }
            attempt++;
            cursorContext.initRead();
            Result result = executor.execute( query, parameters, context );
            eagerResult = new EagerResult( result );
            eagerResult.consume();
            dirtySnapshot = cursorContext.isDirty();
            if ( dirtySnapshot && result.getQueryStatistics().containsUpdates() )
            {
                throw new QueryExecutionKernelException( new UnstableSnapshotException(
                        "Unable to get clean data snapshot for query '%s' that perform updates.", query ) );
            }
        }
        while ( dirtySnapshot );
        return eagerResult;
    }

    private static CursorContext getCursorContext( TransactionalContext context )
    {
        return ((KernelStatement) context.statement()).getCursorContext();
    }

    @FunctionalInterface
    protected interface QueryExecutor
    {
        Result execute( String query, Map<String,Object> parameters, TransactionalContext context ) throws QueryExecutionKernelException;
    }

}
