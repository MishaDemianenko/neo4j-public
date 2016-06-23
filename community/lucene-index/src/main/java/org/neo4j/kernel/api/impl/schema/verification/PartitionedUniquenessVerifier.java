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

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.ReaderSlice;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure;
import org.neo4j.kernel.api.index.PropertyAccessor;

import static java.util.stream.Collectors.toList;

/**
 * A {@link UniquenessVerifier} that is able to verify value uniqueness across multiple index partitions using
 * corresponding {@link PartitionSearcher}s.
 * <p>
 * This verifier reads all terms from all partitions using {@link MultiTerms}, checks document frequency for each term
 * and verifies uniqueness of values from the property store if document frequency is greater than 1.
 *
 * @see MultiTerms
 * @see PartitionSearcher
 * @see DuplicateCheckingCollector
 */
public class PartitionedUniquenessVerifier implements UniquenessVerifier
{
    private final List<PartitionSearcher> searchers;

    public PartitionedUniquenessVerifier( List<PartitionSearcher> searchers )
    {
        this.searchers = searchers;
    }

    @Override
    public void verify( PropertyAccessor accessor, int propKeyId ) throws IndexEntryConflictException, IOException
    {
        for ( FieldInfo fieldInfo : allFields() )
        {
            if ( LuceneDocumentStructure.NODE_ID_KEY.equals( fieldInfo.name ) )
            {
                continue;
            }
            if ( isPointValuesField( fieldInfo ) )
            {

            }
            else
            {
                TermsEnum terms = termsForField( fieldInfo.name ).iterator();
                BytesRef termsRef;
                while ( (termsRef = terms.next()) != null )
                {
                    if ( terms.docFreq() > 1 )
                    {
                        TermQuery query = new TermQuery( new Term( fieldInfo.name, termsRef ) );
                        searchForDuplicates( query, accessor, propKeyId );
                    }
                }
            }
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
        for ( Object propertyValue : updatedPropertyValues )
        {
            Query query = LuceneDocumentStructure.newSeekQuery( propertyValue );
            searchForDuplicates( query, accessor, propKeyId );
        }
    }

    @Override
    public void close() throws IOException
    {
        IOUtils.closeAll( searchers );
    }

    private void pointValues(String fieldName) throws IOException
    {
        for ( LeafReader leafReader : allLeafReaders() )
        {
            //TODO:
        }
    }

    private Terms termsForField( String fieldName ) throws IOException
    {
        List<Terms> terms = new ArrayList<>();
        List<ReaderSlice> readerSlices = new ArrayList<>();

        for ( LeafReader leafReader : allLeafReaders() )
        {
            Fields fields = leafReader.fields();

            Terms leafTerms = fields.terms( fieldName );
            if ( leafTerms != null )
            {
                ReaderSlice readerSlice = new ReaderSlice( 0, Math.toIntExact( leafTerms.size() ), 0 );
                terms.add( leafTerms );
                readerSlices.add( readerSlice );
            }
        }

        Terms[] termsArray = terms.toArray( new Terms[terms.size()] );
        ReaderSlice[] readerSlicesArray = readerSlices.toArray( new ReaderSlice[readerSlices.size()] );

        return new MultiTerms( termsArray, readerSlicesArray );
    }

    private void searchForDuplicates( Query query, PropertyAccessor accessor, int propertyKeyId )
            throws IOException, IndexEntryConflictException
    {
        try
        {
            /**
             * Here {@link DuplicateCheckingCollector#reset()} is deliberately not called to preserve accumulated
             * state (knowledge about duplicates) across all {@link IndexSearcher#search(Query, Collector)} calls.
             */
            DuplicateCheckingCollector collector = new DuplicateCheckingCollector( accessor, propertyKeyId );
            for ( PartitionSearcher searcher : searchers )
            {
                searcher.getIndexSearcher().search( query, collector );
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

    private Set<FieldInfo> allFields()
    {
        return Iterables.stream( allLeafReaders() )
                .map( LeafReader::getFieldInfos )
                .flatMap( fieldInfos -> StreamSupport.stream( fieldInfos.spliterator(), false ) )
                .collect( Collectors.toSet() );
    }

    private List<LeafReader> allLeafReaders()
    {
        return searchers.stream()
                .map( PartitionSearcher::getIndexSearcher )
                .map( IndexSearcher::getIndexReader )
                .flatMap( indexReader -> indexReader.leaves().stream() )
                .map( LeafReaderContext::reader )
                .collect( toList() );
    }
}
