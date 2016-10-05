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
package org.neo4j.consistency.checking;

import java.util.Iterator;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

public class RelationshipChain <RECORD extends RelationshipRecord, REPORT extends ConsistencyReport.RelationshipConsistencyReport>
        implements RecordField<RECORD,REPORT>, ComparativeRecordChecker<RECORD,RelationshipRecord,REPORT>
{

    @Override
    public void checkConsistency( RECORD record, CheckerEngine<RECORD,REPORT> engine, RecordAccess records )
    {
        if ( record.isFirstInFirstChain() )
        {
            PrimitiveLongSet idSet = Primitive.longSet();
            idSet.add( record.getId() );
            Iterator<RelationshipRecord> relationships = records.rawRelationshipChain( record.getFirstNextRel() );
            while ( relationships.hasNext() )
            {
                RelationshipRecord relationshipRecord = relationships.next();
                if ( !idSet.add( relationshipRecord.getId() ))
                {
                    engine.report().relationshipChainCycle( relationshipRecord );
                    return;
                }
                if ( !relationshipRecord.inUse() )
                {
                    engine.report().notUsedRelationshipReferencedInChain( relationshipRecord );
                }
            }
        }
    }

    @Override
    public long valueFrom( RECORD record )
    {
        return record.getFirstNextRel();
    }

    @Override
    public void checkReference( RECORD record, RelationshipRecord relationshipRecord,
            CheckerEngine<RECORD,REPORT> engine, RecordAccess records )
    {
    }
}
