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
package org.neo4j.kernel.api.impl.schema.sampler;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.helpers.TaskControl;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.sampling.NonUniqueIndexSampler;
import org.neo4j.storageengine.api.schema.IndexSample;

/**
 * Sampler for non-unique Lucene schema index.
 * Internally uses terms and their document frequencies for sampling.
 */
public class NonUniqueLuceneIndexSampler extends LuceneIndexSampler
{
    private final IndexSearcher indexSearcher;
    private final IndexSamplingConfig indexSamplingConfig;

    public NonUniqueLuceneIndexSampler( IndexSearcher indexSearcher, TaskControl taskControl,
            IndexSamplingConfig indexSamplingConfig )
    {
        super( taskControl );
        this.indexSearcher = indexSearcher;
        this.indexSamplingConfig = indexSamplingConfig;
    }

    @Override
    protected IndexSample performSampling() throws IndexNotFoundKernelException
    {
        NonUniqueIndexSampler sampler = new NonUniqueIndexSampler( indexSamplingConfig.sampleSizeLimit() );
        IndexReader indexReader = indexSearcher.getIndexReader();
        for ( LeafReaderContext readerContext : indexReader.leaves() )
        {
            try
            {
                Set<String> fieldNames = getFieldNamesToSample( readerContext );
                for ( String fieldName : fieldNames )
                {
                    if ( "number".equals( fieldName ) )
                    {
                        PointValues pointValues = readerContext.reader().getPointValues();
                        pointValues.intersect( fieldName, new PointValues.IntersectVisitor()
                        {
                            @Override
                            public void visit( int docID ) throws IOException
                            {

                            }

                            @Override
                            public void visit( int docID, byte[] packedValue ) throws IOException
                            {
                                sampler.include( DoublePoint.decodeDimension( packedValue, 0 ) + "", 1 );
                            }

                            @Override
                            public PointValues.Relation compare( byte[] minPackedValue, byte[] maxPackedValue )
                            {
                                return null;
                            }
                        } );
                    }
                    else
                    {
                        Terms terms = readerContext.reader().terms( fieldName );
                        if ( terms != null )
                        {
                            TermsEnum termsEnum = terms.iterator();
                            BytesRef termsRef;
                            while ( (termsRef = termsEnum.next()) != null )
                            {
                                sampler.include( termsRef.utf8ToString(), termsEnum.docFreq() );
                                checkCancellation();
                            }
                        }
                    }
                }
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }

        return sampler.result( indexReader.numDocs() );
    }

    private static Set<String> getFieldNamesToSample( LeafReaderContext readerContext ) throws IOException
    {
        FieldInfos fieldInfos = readerContext.reader().getFieldInfos();
        Set<String> fieldNames = new HashSet<>();
        for ( FieldInfo fieldInfo : fieldInfos )
        {
            if ( !LuceneDocumentStructure.NODE_ID_KEY.equals( fieldInfo.name ) )
            {
                fieldNames.add( fieldInfo.name );
            }
        }
        return fieldNames;
    }
}
