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
package org.neo4j.index.legacy;


import org.apache.lucene.index.Term;
import org.apache.lucene.search.suggest.document.FuzzyCompletionQuery;
import org.junit.Test;

import java.util.HashMap;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.index.impl.lucene.legacy.IndexType;
import org.neo4j.test.TestGraphDatabaseFactory;

public class LegacyIndexCustomFieldFactoryIT
{
    static final String TEST_NODE_INDEX = "testNodeIndex";
    static final Label label = Label.label( "testLabel" );

    @Test
    public void queryLegacyIndex() throws Exception
    {
        GraphDatabaseService database = new TestGraphDatabaseFactory().newImpermanentDatabase();
        Index<Node> legacyIndex = getLegacyIndex( database );

        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( label );
            legacyIndex.add( node, "suggest_field", "aaaaa" );
            transaction.success();
        }

        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( label );
            legacyIndex.add( node, "suggest_field", "testa" );
            transaction.success();
        }

        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( label );
            legacyIndex.add( node, "suggest_field", "testb" );
            transaction.success();
        }

        try ( Transaction transaction = database.beginTx() )
        {
            IndexType indexType = IndexType.getIndexType( getIndexConfig() );
            FuzzyCompletionQuery completionQuery =
                    new FuzzyCompletionQuery( indexType.getAnalyzer(), new Term( "suggest_field", "tes" ) );
            IndexHits<Node> hits = legacyIndex.query( completionQuery );
            for ( Node hit : hits )
            {
                System.out.println(hit.getId());
            }
        }
    }

    private Index<Node> getLegacyIndex( GraphDatabaseService database )
    {
        HashMap<String,String> indexConfiguration = getIndexConfig();
        try ( Transaction transaction = database.beginTx() )
        {
            transaction.success();
            return database.index().forNodes( TEST_NODE_INDEX, indexConfiguration );
        }
    }

    private HashMap<String,String> getIndexConfig()
    {
        HashMap<String,String> indexConfiguration = new HashMap<>();
        indexConfiguration.put( "analyzer", CustomTestAnalyzer.class.getName() );
        indexConfiguration.put( "type_factory", SuggestionFieldFactory.class.getName() );
        indexConfiguration.put( "searcher_factory", SuggestionIndexSearcherFactory.class.getName() );
        indexConfiguration.put( "hits_provider", SuggestionIndexHitsProvider.class.getName() );
        indexConfiguration.put( "codec", TestSuggestionCodec.class.getName() );
        return indexConfiguration;
    }

}
