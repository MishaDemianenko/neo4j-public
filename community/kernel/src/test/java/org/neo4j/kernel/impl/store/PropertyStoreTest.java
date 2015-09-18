/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;

import org.neo4j.helpers.Pair;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.JumpingIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.DefaultFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PropertyStoreTest
{
    @ClassRule
    public static PageCacheRule pageCacheRule = new PageCacheRule( false );

    @Rule
    public final DefaultFileSystemRule fsRule = new DefaultFileSystemRule();
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private DynamicStringStore dynamicStringStore;
    private PropertyStore propertyStore;
    private DynamicArrayStore dynamicArrayStore;

    @Before
    public void setUp() throws IOException
    {
        PageCache pageCache = pageCacheRule.getPageCache( fsRule.get() );
        dynamicStringStore = mock( DynamicStringStore.class );
        dynamicArrayStore = mock( DynamicArrayStore.class );
        File propertyFile = temporaryFolder.newFile( PropertyStore.TYPE_DESCRIPTOR );

        propertyStore = new PropertyStore( propertyFile, new Config(), new
                JumpingIdGeneratorFactory( 1 ), pageCache, NullLogProvider.getInstance(),
                dynamicStringStore, mock( PropertyKeyTokenStore.class ), dynamicArrayStore,
                StoreVersionMismatchHandler.FORCE_CURRENT_VERSION );
        propertyStore.initialise( true );
        propertyStore.makeStoreOk();
    }

    @After
    public void tearDown()
    {
        propertyStore.close();
    }

    @Test
    public void readingEmptyStringFromPropertyBlock()
    {
        PropertyBlock propertyBlock = new PropertyBlock();
        PropertyType testPropertyType = PropertyType.STRING;
        PropertyStore.setSingleBlockValue( propertyBlock, 1, testPropertyType, 0 );

        AbstractDynamicStore.DynamicRecordCursor recordCursor = mock( AbstractDynamicStore.DynamicRecordCursor.class );
        when( dynamicStringStore.getRecordsCursor( anyLong(), anyBoolean() ) ).thenReturn( recordCursor );
        when( dynamicStringStore.readFullByteArray( anyListOf( DynamicRecord.class ), eq( testPropertyType ) ) )
                .thenReturn( Pair.<byte[],byte[]>empty() );

        assertEquals( StringUtils.EMPTY, propertyStore.getStringFor( propertyBlock ) );
    }

    @Test
    public void readingNullArrayFromPropertyBlock()
    {
        PropertyBlock propertyBlock = new PropertyBlock();
        PropertyType testPropertyType = PropertyType.ARRAY;
        PropertyStore.setSingleBlockValue( propertyBlock, 1, testPropertyType, 0 );

        AbstractDynamicStore.DynamicRecordCursor recordCursor = mock( AbstractDynamicStore.DynamicRecordCursor.class );
        when( dynamicArrayStore.getRecordsCursor( anyLong(), anyBoolean() ) ).thenReturn( recordCursor );
        when( dynamicArrayStore.readFullByteArray( anyListOf( DynamicRecord.class ), eq( testPropertyType ) ) )
                .thenReturn( Pair.<byte[],byte[]>empty() );

        assertNull( propertyStore.getArrayFor( propertyBlock ) );
    }

    @Test
    public void shouldWriteOutTheDynamicChainBeforeUpdatingThePropertyRecord() throws IOException
    {
        final long propertyRecordId = propertyStore.nextId();

        PropertyRecord record = new PropertyRecord( propertyRecordId );
        record.setInUse( true );

        DynamicRecord dynamicRecord = dynamicRecord();
        PropertyBlock propertyBlock = propertyBlockWith( dynamicRecord );
        record.setPropertyBlock( propertyBlock );

        doAnswer( new Answer()
        {
            @Override
            public Object answer( InvocationOnMock invocation ) throws Throwable
            {
                PropertyRecord recordBeforeWrite = propertyStore.forceGetRecord( propertyRecordId );
                assertFalse( recordBeforeWrite.inUse() );
                return null;
            }
        } ).when( dynamicStringStore ).updateRecord( dynamicRecord );

        // when
        propertyStore.updateRecord( record );

        // then verify that our mocked method above, with the assert, was actually called
        verify( dynamicStringStore ).updateRecord( dynamicRecord );

    }

    private DynamicRecord dynamicRecord()
    {
        DynamicRecord dynamicRecord = new DynamicRecord( 42 );
        dynamicRecord.setType( PropertyType.STRING.intValue() );
        dynamicRecord.setCreated();
        return dynamicRecord;
    }

    private PropertyBlock propertyBlockWith( DynamicRecord dynamicRecord )
    {
        PropertyBlock propertyBlock = new PropertyBlock();

        PropertyKeyTokenRecord key = new PropertyKeyTokenRecord( 10 );
        PropertyStore.setSingleBlockValue( propertyBlock, key.getId(), PropertyType.STRING, dynamicRecord.getId() );
        propertyBlock.addValueRecord( dynamicRecord );

        return propertyBlock;
    }
}
