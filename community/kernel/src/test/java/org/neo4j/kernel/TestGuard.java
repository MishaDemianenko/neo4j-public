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
package org.neo4j.kernel;

import org.junit.Test;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.guard.TimeoutGuard;
import org.neo4j.kernel.impl.util.UnsatisfiedDependencyException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertNotNull;

@SuppressWarnings("deprecation"/*getGuard() is deprecated (GraphDatabaseAPI), and used all throughout this test*/)
public class TestGuard
{
    @Test(expected = UnsatisfiedDependencyException.class)
    public void testGuardNotInsertedByDefault()
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        try
        {
            getGuard( db );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void testGuardInsertedByDefault()
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().
                newImpermanentDatabaseBuilder().
                setConfig( GraphDatabaseSettings.execution_guard_enabled, Settings.TRUE ).
                newGraphDatabase();
        assertNotNull( getGuard( db ) );
        db.shutdown();
    }

    private Guard getGuard( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( TimeoutGuard.class );
    }
}
