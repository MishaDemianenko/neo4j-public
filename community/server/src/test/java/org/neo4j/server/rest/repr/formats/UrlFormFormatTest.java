/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.rest.repr.formats;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class UrlFormFormatTest
{
    @Test
    void shouldParseEmptyMap() throws Exception
    {
        UrlFormFormat format = new UrlFormFormat();
        Map<String, Object> map = format.readMap( "" );

        assertThat( map.size(), is( 0 ) );
    }

    @Test
    void canParseSingleKeyMap() throws Exception
    {
        UrlFormFormat format = new UrlFormFormat();
        Map<String, Object> map = format.readMap( "var=A" );

        assertThat( map.size(), is( 1 ) );
        assertThat( map.get( "var" ), is( "A" ) );
    }

    @Test
    void canParseListsInMaps() throws Exception
    {
        UrlFormFormat format = new UrlFormFormat();
        Map<String, Object> map = format.readMap( "var=A&var=B" );

        assertThat( map.size(), is( 1 ) );
        assertThat( ( (List<String>) map.get( "var" ) ).get( 0 ), is( "A" ) );
        assertThat( ( (List<String>) map.get( "var" ) ).get( 1 ), is( "B" ) );
    }

    @Test
    void shouldSupportPhpStyleUrlEncodedPostBodiesForAListOnly() throws Exception
    {
        UrlFormFormat format = new UrlFormFormat();
        Map<String, Object> map = format.readMap( "var[]=A&var[]=B" );

        assertThat( map.size(), is( 1 ) );
        assertThat( ( (List<String>) map.get( "var" ) ).get( 0 ), is( "A" ) );
        assertThat( ( (List<String>) map.get( "var" ) ).get( 1 ), is( "B" ) );

    }

    @Test
    void shouldSupportPhpStyleUrlEncodedPostBodiesForAListAndNonListParams() throws Exception
    {
        UrlFormFormat format = new UrlFormFormat();
        Map<String, Object> map = format.readMap( "var[]=A&var[]=B&foo=bar" );

        assertThat( map.size(), is( 2 ) );
        assertThat( ( (List<String>) map.get( "var" ) ).get( 0 ), is( "A" ) );
        assertThat( ( (List<String>) map.get( "var" ) ).get( 1 ), is( "B" ) );
        assertEquals( "bar", map.get( "foo" ) );
    }

}
