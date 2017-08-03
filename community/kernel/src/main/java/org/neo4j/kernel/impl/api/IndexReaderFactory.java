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
package org.neo4j.kernel.impl.api;

import java.util.LinkedList;

import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.storageengine.api.schema.IndexReader;

public class IndexReaderFactory
{
    private final IndexingService indexingService;
    private final LinkedList<IndexReader> readers = new LinkedList<>();

    public IndexReaderFactory( IndexingService indexingService )
    {
        this.indexingService = indexingService;
    }

    public IndexReader newReader( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        IndexProxy index = indexingService.getIndexProxy( descriptor.schema() );
        IndexReader indexReader = index.newReader();
        readers.add( indexReader );
        return indexReader;
    }

    public void close()
    {
        readers.forEach( IndexReader::close );
    }
}
