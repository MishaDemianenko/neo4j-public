package org.neo4j.test;

public class Main
{

    public static void main( String[] args ) throws Exception
    {
        System.out.println( "Start loader test" );

        EmbeddedJarLoader customLoader = new EmbeddedJarLoader( "lib/commons-collections-3.2.1.jar" );
        Class clazz = customLoader.loadClass( "org.apache.commons.collections.bag.HashBag" );
        Object object = clazz.newInstance();
        System.out.println( object.getClass().getName() );
        customLoader.close();

        try
        {
            Class.forName( "org.apache.commons.collections.bag.HashBag" );
        }
        catch ( Exception e )
        {
            System.out.println( "It's still not possible to load class directly." );
        }

        System.out.println( "loader test completed" );
    }
}
