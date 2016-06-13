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
package org.neo4j.kernel.api.impl.schema.verification;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure;
import org.neo4j.kernel.api.index.PreexistingIndexEntryConflictException;
import org.neo4j.kernel.api.index.PropertyAccessor;

/**
 * A {@link UniquenessVerifier} that is able to verify value uniqueness inside a single index partition using
 * it's {@link PartitionSearcher}.
 * <p>
 * This verifier reads all terms, checks document frequency for each term and verifies uniqueness of values from the
 * property store if document frequency is greater than 1.
 *
 * @see PartitionSearcher
 * @see DuplicateCheckingCollector
 */
public class SimpleUniquenessVerifier implements UniquenessVerifier
{
    private final PartitionSearcher partitionSearcher;

    public SimpleUniquenessVerifier( PartitionSearcher partitionSearcher )
    {
        this.partitionSearcher = partitionSearcher;
    }

    @Override
    public void verify( PropertyAccessor accessor, int propKeyId ) throws IndexEntryConflictException, IOException
    {
        try
        {
            DuplicateCheckingCollector collector = new DuplicateCheckingCollector( accessor, propKeyId );
            IndexSearcher searcher = indexSearcher();
            for ( LeafReaderContext leafReaderContext : searcher.getIndexReader().leaves() )
            {
                LeafReader reader = leafReaderContext.reader();
                FieldInfos fieldInfos = reader.getFieldInfos();
                for ( FieldInfo fieldInfo : fieldInfos )
                {
                    if ( LuceneDocumentStructure.NODE_ID_KEY.equals( fieldInfo.name ) )
                    {
                        continue;
                    }
                    if ( isPointValuesField( fieldInfo ) )
                    {
                        PointValues pointValues = reader.getPointValues();
                        pointValues.intersect( fieldInfo.name, new UniquenessIntersectVisitor( searcher,
                                fieldInfo.getPointDimensionCount(),
                                fieldInfo.getPointNumBytes() ) );
                    }
                    else
                    {
                        Fields fields = reader.fields();
                        TermsEnum terms = fields.terms( fieldInfo.name ).iterator();
                        BytesRef termsRef;
                        while ( (termsRef = terms.next()) != null )
                        {
                            if ( terms.docFreq() > 1 )
                            {
                                collector.reset();
                                searcher.search( new TermQuery( new Term( fieldInfo.name, termsRef ) ), collector );
                            }
                        }
                    }
                }
            }
        }
        catch ( IOException e )
        {
            Throwable cause = e.getCause();
            if ( cause instanceof IndexEntryConflictException )
            {
                throw (IndexEntryConflictException) cause;
            }
            throw e;
        }
    }

    private boolean isPointValuesField( FieldInfo fieldInfo )
    {
        return (fieldInfo.getPointDimensionCount() > 0) && (fieldInfo.getPointNumBytes() > 0);
    }

    @Override
    public void verify( PropertyAccessor accessor, int propKeyId, List<Object> updatedPropertyValues )
            throws IndexEntryConflictException, IOException
    {
        try
        {
            DuplicateCheckingCollector collector = new DuplicateCheckingCollector( accessor, propKeyId );
            for ( Object propertyValue : updatedPropertyValues )
            {
                collector.reset();
                Query query = LuceneDocumentStructure.newSeekQuery( propertyValue );
                indexSearcher().search( query, collector );
            }
        }
        catch ( IOException e )
        {
            Throwable cause = e.getCause();
            if ( cause instanceof IndexEntryConflictException )
            {
                throw (IndexEntryConflictException) cause;
            }
            throw e;
        }
    }

    @Override
    public void close() throws IOException
    {
        partitionSearcher.close();
    }

    private IndexSearcher indexSearcher()
    {
        return partitionSearcher.getIndexSearcher();
    }

    private static class UniquenessIntersectVisitor implements PointValues.IntersectVisitor
    {
        private int previousDocId;
        private byte[] buffer;
        private final IndexSearcher searcher;
        private final int dimensionCount;
        private int bytesPerDimension;

        public UniquenessIntersectVisitor( IndexSearcher searcher, int dimensionCount, int bytesPerDimension )
        {
            this.searcher = searcher;
            this.dimensionCount = dimensionCount;
            this.bytesPerDimension = bytesPerDimension;
            buffer = new byte[dimensionCount * bytesPerDimension];
        }

        @Override
        public void visit( int docID ) throws IOException
        {
        }

        @Override
        public void visit( int docID, byte[] packedValue ) throws IOException
        {
            if ( Arrays.equals( buffer, packedValue ) )
            {
                long nodeId = LuceneDocumentStructure.getNodeId( searcher.doc( docID ) );
                long bufferedNodeId = LuceneDocumentStructure.getNodeId( searcher.doc( previousDocId ) );
                throw new IOException(  new PreexistingIndexEntryConflictException( DoublePoint.decodeDimension( packedValue, 0),
                    nodeId, bufferedNodeId ));
            }
            previousDocId = docID;
            System.arraycopy( packedValue, 0, buffer, 0, packedValue.length);
        }

        @Override
        public PointValues.Relation compare( byte[] minPackedValue, byte[] maxPackedValue )
        {
            return null;
        }
    }
}
