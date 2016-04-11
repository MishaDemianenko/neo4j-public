package org.neo4j.test.imports;

import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.StubPagedFile;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.IntStoreHeader;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.impl.EphemeralIdGenerator;
import org.neo4j.unsafe.impl.batchimport.CalculateDenseNodesStage;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.CountingStoreUpdateMonitor;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.RelationshipLinkbackStage;
import org.neo4j.unsafe.impl.batchimport.RelationshipStage;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers;
import org.neo4j.unsafe.impl.batchimport.input.BadCollector;
import org.neo4j.unsafe.impl.batchimport.input.InputCache;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionSupervisors;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;
import org.neo4j.unsafe.impl.batchimport.store.io.IoMonitor;
import org.neo4j.unsafe.impl.batchimport.store.io.IoTracer;
import org.neo4j.unsafe.impl.internal.dragons.FeatureToggles;

import static org.mockito.Matchers.*;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.AUTO;

public class RelationshipStageTest
{

    public static final int PAGE_SIZE = 4096;
    private final long minId = FeatureToggles.getLong( getClass(), "relationship.minId", 900_000_000 );

    @Test
    public void testImportIdOverlap() throws Exception
    {
        Configuration configuration = Configuration.DEFAULT;
        IoMonitor ioMonitor = new IoMonitor( IoTracer.NONE );
        BatchingNeoStores batchingNeoStores = Mockito.mock( BatchingNeoStores.class );
        PageCache pageCache = Mockito.mock( PageCache.class );
        Mockito.when( pageCache.pageSize() ).thenReturn( PAGE_SIZE );
        Mockito.when( pageCache.map( any( File.class ), eq( PAGE_SIZE ) ) ).thenReturn( new StubPagedFile( PAGE_SIZE ) );

        RelationshipStore relationshipStore = new RelationshipStore( new File( "test" ), Config.defaults(),
                getIdGeneratorFactory(), pageCache, NullLogProvider.getInstance(), HighLimit.RECORD_FORMATS );
        relationshipStore.loadStorage();


        Mockito.when( batchingNeoStores.getRelationshipStore() ).thenReturn( relationshipStore );

        DynamicArrayStore arrayStore = Mockito.mock( DynamicArrayStore.class );
        Mockito.when( arrayStore.getRecordSize() ).thenReturn( getDynamicRecordSize() );

        DynamicStringStore stringStore = Mockito.mock( DynamicStringStore.class );
        Mockito.when( stringStore.getRecordSize() ).thenReturn( getDynamicRecordSize() );

        PropertyStore propertyStore = Mockito.mock( PropertyStore.class );
        Mockito.when( propertyStore.getArrayStore() ).thenReturn( arrayStore );
        Mockito.when( propertyStore.getStringStore() ).thenReturn( stringStore );

        Mockito.when( batchingNeoStores.getPropertyStore() ).thenReturn( propertyStore );

        NodeRelationshipCache nodeRelationshipCache = new NodeRelationshipCache( AUTO, configuration.denseNodeThreshold() );

        while (true)
        {

            batchingNeoStores.getPropertyStore().setHighestPossibleIdInUse( minId );

            IdMapper idMapper = IdMappers.actual();
            InputIterable<InputRelationship> relationships = getRelationships();
            CalculateDenseNodesStage calculateDenseNodesStage =
                    new CalculateDenseNodesStage( configuration, relationships,
                            nodeRelationshipCache, idMapper, relationshipStore, new BadCollector( System.out, 2, 7 ),
                            Mockito.mock( InputCache.class ) );
            execute( calculateDenseNodesStage );

            RelationshipStage relationshipStage =
                    new RelationshipStage( configuration, ioMonitor, relationships, idMapper,
                            batchingNeoStores, nodeRelationshipCache, false, new CountingStoreUpdateMonitor(), minId );

            execute( relationshipStage );

            RelationshipLinkbackStage linkbackStage =
                    new RelationshipLinkbackStage( configuration, relationshipStore, nodeRelationshipCache );
            execute( linkbackStage );
        }
    }

    private IdGeneratorFactory getIdGeneratorFactory()
    {
        return new IdGeneratorFactory()
        {
            @Override
            public org.neo4j.kernel.impl.store.id.IdGenerator open( File filename, int grabSize, IdType idType,
                    long highId, long maxId )
            {
                return getEphemeralIdGenerator( idType );
            }

            private EphemeralIdGenerator getEphemeralIdGenerator( IdType idType )
            {
                EphemeralIdGenerator generator = new EphemeralIdGenerator( idType );
                generator.setHighId( minId );
                return generator;
            }

            @Override
            public void create( File filename, long highId, boolean throwIfFileExists )
            {
            }

            @Override
            public org.neo4j.kernel.impl.store.id.IdGenerator get( IdType idType )
            {
                return getEphemeralIdGenerator( idType );
            }
        };
    }

    private int getDynamicRecordSize()
    {
        return HighLimit.RECORD_FORMATS.dynamic().getRecordSize( new IntStoreHeader( 7 ) );
    }

    private void execute(Stage... stages)
    {
        ExecutionSupervisors.superviseExecution( ExecutionMonitors.invisible(), Configuration.DEFAULT, stages );
    }

    private InputIterable<InputRelationship> getRelationships()
    {
        return new InputIterable<InputRelationship>()
        {
            @Override
            public InputIterator<InputRelationship> iterator()
            {
                return new InputRelationshipIterator();
            }

            @Override
            public boolean supportsMultiplePasses()
            {
                return true;
            }
        };
    }

    private class InputRelationshipIterator extends InputIterator.Adapter<InputRelationship>
    {
        private long relationship = minId;
        private long nodeId = minId;
        private int position = 0;

        @Override
        protected InputRelationship fetchNextOrNull()
        {
            if (relationship > minId + 1_00000)
            {
                return null;
            }
            if (relationship % 100000 == 0)
            {
                System.out.println(relationship);
                position = 0;
            }
            if (relationship % 2 == 0)
            {
                nodeId++;
            }
            return new InputRelationship( "", relationship++, position++, new Object[]{String.valueOf( relationship ),
                    relationship}, relationship - 5,
                    nodeId, nodeId - 1, "type", 2069 );
        }
    }

}