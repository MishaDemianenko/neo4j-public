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
package org.neo4j.store.watch;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.index.impl.lucene.legacy.LuceneDataSource;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.fs.watcher.FileWatcher;
import org.neo4j.io.fs.watcher.event.FileWatchEventListenerAdapter;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.util.watcher.DefaultFileDeletionEventListener;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class FileWatchIT
{

    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    private File storeDir;
    private AssertableLogProvider logProvider;
    private GraphDatabaseService database;

    @Before
    public void setUp()
    {
        storeDir = testDirectory.graphDbDir();
        logProvider = new AssertableLogProvider( true );
        database = new TestGraphDatabaseFactory().setInternalLogProvider( logProvider ).newEmbeddedDatabase( storeDir );
    }

    @After
    public void tearDown()
    {
        shutdownDatabaseSilently( database );
    }

    @Test
    public void notifyAboutStoreFileDeletion() throws Exception
    {
        String fileName = MetaDataStore.DEFAULT_NAME;
        FileWatcher fileWatcher = getFileWatcher( (GraphDatabaseAPI) database );
        DeletionLatchEventListener deletionListener = new DeletionLatchEventListener( fileName );
        fileWatcher.addFileWatchEventListener( deletionListener );

        createNode( database );
        deletionListener.awaitModificationNotification();

        deleteFile( storeDir, fileName );
        deletionListener.awaitDeletionNotification();

        logProvider.assertContainsMessageContaining(
                "Store file '" + fileName + "' was deleted while database was running." );
    }

    @Test
    public void notifyWhenFileWatchingFailToStart()
    {
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        GraphDatabaseService db = null;
        try
        {
            db = new TestGraphDatabaseFactory().setInternalLogProvider( logProvider )
                    .setFileSystem( new NonWatchableFileSystemAbstraction() )
                    .newEmbeddedDatabase( testDirectory.directory( "faied-start-db" ) );

            logProvider.assertContainsMessageContaining( "Can not create file watcher for current file system. " +
                    "File monitoring capabilities for store files will be disabled." );
        }
        finally
        {
            shutdownDatabaseSilently( db );
        }
    }

    @Test
    public void notifyAboutLegacyIndexFolderRemoval() throws InterruptedException, IOException
    {
        String monitoredDirectory = getLegacyIndexDirectory( storeDir );

        FileWatcher fileWatcher = getFileWatcher( (GraphDatabaseAPI) database );
        DeletionLatchEventListener deletionListner = new DeletionLatchEventListener( monitoredDirectory );
        fileWatcher.addFileWatchEventListener( deletionListner );

        createNode( database );
        deletionListner.awaitModificationNotification();

        deleteStoreDirectory( storeDir, monitoredDirectory );
        deletionListner.awaitDeletionNotification();

        logProvider.assertContainsMessageContaining(
                "Store directory '" + monitoredDirectory + "' was deleted while database was running." );
    }

    @Test
    public void doNotNotifyAboutLuceneIndexFilesDeletion() throws InterruptedException, IOException
    {
        DependencyResolver dependencyResolver = ((GraphDatabaseAPI) database).getDependencyResolver();
        FileWatcher fileWatcher = dependencyResolver.resolveDependency( FileWatcher.class );
        CheckPointer checkPointer = dependencyResolver.resolveDependency( CheckPointer.class );

        String propertyStoreName = MetaDataStore.DEFAULT_NAME + StoreFactory.PROPERTY_STORE_NAME;
        AccumulativeDeletionEventListener accumulativeListener = new AccumulativeDeletionEventListener();
        ModificationEventListener modificationListener = new ModificationEventListener( propertyStoreName );
        fileWatcher.addFileWatchEventListener( modificationListener );
        fileWatcher.addFileWatchEventListener( accumulativeListener );

        String labelName = "labelName";
        String propertyName = "propertyName";
        Label testLabel = Label.label( labelName );
        createIndexes( database, propertyName, testLabel );
        createNode( database, propertyName, testLabel );
        forceCheckpoint( checkPointer );
        modificationListener.awaitModificationNotification();

        fileWatcher.removeFileWatchEventListener( modificationListener );
        ModificationEventListener afterRemovalListener = new ModificationEventListener( propertyStoreName );
        fileWatcher.addFileWatchEventListener( afterRemovalListener );

        dropAllIndexes( database );
        createNode( database, propertyName, testLabel );
        forceCheckpoint( checkPointer );
        afterRemovalListener.awaitModificationNotification();

        accumulativeListener.assertDoesNotHaveAnyDeletions();
    }

    @Test
    public void doNotMonitorTransactionLogFiles() throws InterruptedException
    {
        FileWatcher fileWatcher = getFileWatcher( (GraphDatabaseAPI) database );
        ModificationEventListener modificationEventListener =
                new ModificationEventListener( MetaDataStore.DEFAULT_NAME );
        fileWatcher.addFileWatchEventListener( modificationEventListener );

        createNode( database );
        modificationEventListener.awaitModificationNotification();

        String fileName = PhysicalLogFile.DEFAULT_NAME + ".0";
        DeletionLatchEventListener deletionListener = new DeletionLatchEventListener( fileName );
        fileWatcher.addFileWatchEventListener( deletionListener );
        deleteFile( storeDir, fileName );
        deletionListener.awaitDeletionNotification();

        AssertableLogProvider.LogMatcher logMatcher =
                AssertableLogProvider.inLog( DefaultFileDeletionEventListener.class )
                        .info( containsString( fileName ) );
        logProvider.assertNone( logMatcher );
    }

    @Test
    public void notifyWhenWholeStoreDirectoryRemoved() throws IOException, InterruptedException
    {
        String fileName = MetaDataStore.DEFAULT_NAME;
        FileWatcher fileWatcher = getFileWatcher( (GraphDatabaseAPI) database );
        ModificationEventListener modificationListener = new ModificationEventListener( fileName );
        fileWatcher.addFileWatchEventListener( modificationListener );
        createNode( database );

        modificationListener.awaitModificationNotification();
        fileWatcher.removeFileWatchEventListener( modificationListener );

        String storeDirectoryName = TestDirectory.DATABASE_DIRECTORY;
        DeletionLatchEventListener eventListener = new DeletionLatchEventListener( storeDirectoryName );
        fileWatcher.addFileWatchEventListener( eventListener );
        FileUtils.deleteRecursively( storeDir );
        eventListener.awaitDeletionNotification();

        logProvider.assertContainsMessageContaining(
                "Store directory '" + storeDirectoryName + "' was deleted while database was running." );
    }

    private void shutdownDatabaseSilently( GraphDatabaseService databaseService )
    {
        if ( databaseService != null )
        {
            try
            {
                databaseService.shutdown();
            }
            catch ( Exception expected )
            {
                // ignored
            }
        }
    }

    private void dropAllIndexes( GraphDatabaseService database )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            for ( IndexDefinition definition : database.schema().getIndexes() )
            {
                definition.drop();
            }
            transaction.success();
        }
    }

    private void createIndexes( GraphDatabaseService database, String propertyName, Label testLabel )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            database.schema().indexFor( testLabel ).on( propertyName ).create();
            transaction.success();
        }

        try ( Transaction ignored = database.beginTx() )
        {
            database.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
        }
    }

    private void forceCheckpoint( CheckPointer checkPointer ) throws IOException
    {
        checkPointer.forceCheckPoint( new SimpleTriggerInfo( "testForceCheckPoint" ) );
    }

    private String getLegacyIndexDirectory( File storeDir )
    {
        File schemaIndexDirectory = LuceneDataSource.getLuceneIndexStoreDirectory( storeDir );
        Path relativeIndexPath = storeDir.toPath().relativize( schemaIndexDirectory.toPath() );
        return relativeIndexPath.getName( 0 ).toString();
    }

    private void createNode( GraphDatabaseService database, String propertyName, Label testLabel )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( testLabel );
            node.setProperty( propertyName, "value" );
            transaction.success();
        }
    }

    private FileWatcher getFileWatcher( GraphDatabaseAPI database )
    {
        DependencyResolver dependencyResolver = database.getDependencyResolver();
        return dependencyResolver.resolveDependency( FileWatcher.class );
    }

    private void deleteFile( File storeDir, String fileName )
    {
        File metadataStore = new File( storeDir, fileName );
        FileUtils.deleteFile( metadataStore );
    }

    private void deleteStoreDirectory( File storeDir, String directoryName ) throws IOException
    {
        File directory = new File( storeDir, directoryName );
        FileUtils.deleteRecursively( directory );
    }

    private void createNode( GraphDatabaseService database )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            database.createNode();
            transaction.success();
        }
    }

    private static class NonWatchableFileSystemAbstraction extends DefaultFileSystemAbstraction
    {
        @Override
        public FileWatcher fileWatcher() throws IOException
        {
            throw new IOException( "You can't watch me!" );
        }
    }

    private static class AccumulativeDeletionEventListener extends FileWatchEventListenerAdapter
    {
        private List<String> deletedFiles = new ArrayList<>();

        @Override
        public void fileDeleted( String fileName )
        {
            deletedFiles.add( fileName );
        }

        void assertDoesNotHaveAnyDeletions()
        {
            assertThat( "Should not have any deletions registered", deletedFiles, Matchers.empty() );
        }
    }

    private static class ModificationEventListener extends FileWatchEventListenerAdapter
    {
        final String expectedFileName;
        private final CountDownLatch modificationLatch = new CountDownLatch( 1 );

        ModificationEventListener( String expectedFileName )
        {
            this.expectedFileName = expectedFileName;
        }

        @Override
        public void fileModified( String fileName )
        {
            if ( expectedFileName.equals( fileName ) )
            {
                modificationLatch.countDown();
            }
        }

        void awaitModificationNotification() throws InterruptedException
        {
            modificationLatch.await();
        }
    }

    private static class DeletionLatchEventListener extends ModificationEventListener
    {
        private final CountDownLatch deletionLatch = new CountDownLatch( 1 );

        DeletionLatchEventListener( String expectedFileName )
        {
            super( expectedFileName );
        }

        @Override
        public void fileDeleted( String fileName )
        {
            assertTrue( fileName.endsWith( expectedFileName ) );
            deletionLatch.countDown();
        }

        void awaitDeletionNotification() throws InterruptedException
        {
            deletionLatch.await();
        }

    }
}