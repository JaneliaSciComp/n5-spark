package org.janelia.saalfeldlab.n5.spark;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.DefaultBlockWriter;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.downsample.N5DownsamplerSpark;
import org.janelia.saalfeldlab.n5.spark.supplier.N5ReaderSupplier;
import org.janelia.saalfeldlab.n5.spark.supplier.N5WriterSupplier;
import org.janelia.saalfeldlab.n5.spark.util.CmdUtils;
import org.janelia.saalfeldlab.n5.spark.util.N5Compression;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;

import org.janelia.saalfeldlab.n5.imglib2.N5CellLoader;

import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.Dimensions;
import net.imglib2.converter.Converter;
import net.imglib2.converter.Converters;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.NativeType;
import net.imglib2.type.Type;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.ByteType;
import net.imglib2.type.numeric.integer.IntType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.integer.ShortType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedIntType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;
import net.imglib2.view.Views;

import net.imglib2.IterableInterval;
import net.imglib2.cache.Cache;
import net.imglib2.cache.LoaderCache;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.cache.img.DiskCachedCellImgFactory;
import net.imglib2.cache.img.DiskCachedCellImgOptions;
import net.imglib2.cache.img.LoadedCellCacheLoader;
import net.imglib2.cache.ref.BoundedSoftRefLoaderCache;
import net.imglib2.cache.ref.SoftRefLoaderCache;
import net.imglib2.exception.ImgLibException;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.AccessFlags;
import net.imglib2.img.basictypeaccess.ArrayDataAccessFactory;
import net.imglib2.img.basictypeaccess.array.ArrayDataAccess;
import net.imglib2.img.basictypeaccess.volatiles.VolatileAccess;
import net.imglib2.img.cell.Cell;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.cell.LazyCellImg;
import net.imglib2.iterator.IntervalIterator;
import net.imglib2.loops.LoopBuilder;
import net.imglib2.type.NativeType;
import net.imglib2.type.Type;
import net.imglib2.type.label.LabelMultisetType;
import net.imglib2.type.numeric.integer.ByteType;
import net.imglib2.type.numeric.integer.IntType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.integer.ShortType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedIntType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.util.Util;
import net.imglib2.util.ValuePair;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.supplier.N5WriterSupplier;
import org.janelia.saalfeldlab.n5.spark.util.CmdUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;

import bdv.export.Downsample;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.Views;
import scala.xml.Null;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.stream.Stream;

import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.neighborhood.Neighborhood;
import net.imglib2.algorithm.neighborhood.RectangleNeighborhoodFactory;
import net.imglib2.algorithm.neighborhood.RectangleNeighborhoodUnsafe;
import net.imglib2.algorithm.neighborhood.RectangleShape;
import net.imglib2.type.numeric.RealType;
import net.imglib2.view.Views;

import mpicbg.spim.data.sequence.FinalVoxelDimensions;

import static java.util.Comparator.*;

public class N5ToVVDSpark
{
    private static final int MAX_PARTITIONS = 15000;

    public static final String DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY = "downsamplingFactors";
    public static final String PIXEL_RESOLUTION_ATTRIBUTE_KEY = "pixelResolution";


    protected static class LockedFileChannel implements Closeable {

        private final FileChannel channel;

        public static LockedFileChannel openForReading(final Path path) throws IOException {

            return new LockedFileChannel(path, true, false);
        }

        public static LockedFileChannel openForWriting(final Path path) throws IOException {

            return new LockedFileChannel(path, false, false);
        }

        public static LockedFileChannel openForAppend(final Path path) throws IOException {

            return new LockedFileChannel(path, false, true);
        }

        private LockedFileChannel(final Path path, final boolean readOnly, final boolean append) throws IOException {

            final OpenOption[] options = readOnly ? new OpenOption[]{StandardOpenOption.READ} :
                    (append ? new OpenOption[]{StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND} : new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE});
            channel = FileChannel.open(path, options);

            for (boolean waiting = true; waiting;) {
                waiting = false;
                try {
                    channel.lock(0L, Long.MAX_VALUE, readOnly);
                } catch (final OverlappingFileLockException e) {
                    waiting = true;
                    try {
                        //System.err.println("LockedFileChannel: wait");
                        Thread.sleep(100);
                    } catch (final InterruptedException f) {
                        waiting = false;
                        Thread.currentThread().interrupt();
                        System.err.println("LockedFileChannel: interrupted");
                    }
                } catch (final IOException e) {
                    System.err.println(e);
                }
            }
        }

        public FileChannel getFileChannel() {

            return channel;
        }

        public long getFileSize() {
            try {
                return channel.size();
            } catch (final IOException e) {
                return -1L;
            }
        }

        @Override
        public void close() throws IOException {

            channel.close();
        }
    }

    /**
     * Downsamples the given input dataset of an N5 container with respect to the given downsampling factors.
     * The output dataset will be created within the same N5 container with given block size.
     *
     * @param sparkContext
     * @param n5Supplier
     * @param inputDatasetPath
     * @param outputDatasetPath
     * @param downsamplingFactors
     * @param blockSize
     * @param overwriteExisting
     * @throws IOException
     */
    public static < I extends NativeType< I > & RealType< I >, O extends NativeType< O > & RealType< O > > List<VVDBlockMetadata> downsample(
            final JavaSparkContext sparkContext,
            final N5ReaderSupplier n5InputSupplier,
            final String inputDatasetPath,
            final N5WriterSupplier n5OutputSupplier,
            final String outputDatasetPath,
            final int[] downsamplingFactors,
            final Optional< int[] > blockSizeOptional,
            final Optional< Compression > compressionOptional,
            final Optional< DataType > dataTypeOptional,
            final Optional< Pair< Double, Double > > valueRangeOptional,
            final boolean overwriteExisting ) throws IOException
    {
        final N5Reader n5Input = n5InputSupplier.get();
        final DatasetAttributes inputAttributes = n5Input.getDatasetAttributes( inputDatasetPath );

        final int[] inputBlockSize = inputAttributes.getBlockSize();
        final Compression inputCompression = inputAttributes.getCompression();
        final DataType inputDataType = inputAttributes.getDataType();

        final int[] outputBlockSize = blockSizeOptional.isPresent() ? blockSizeOptional.get() : inputBlockSize;
        final Compression outputCompression = compressionOptional.isPresent() ? compressionOptional.get() : inputCompression;
        final DataType outputDataType = dataTypeOptional.isPresent() ? dataTypeOptional.get() : inputDataType;

        final N5Writer n5Output = n5OutputSupplier.get();
        if ( n5Output.datasetExists( outputDatasetPath ) )
        {
            if ( !overwriteExisting )
            {
                throw new RuntimeException( "Output dataset already exists: " + outputDatasetPath );
            }
            else
            {
                // Requested to overwrite an existing dataset, make sure that the block sizes match
                final int[] oldOutputBlockSize = n5Output.getDatasetAttributes( outputDatasetPath ).getBlockSize();
                if ( !Arrays.equals( outputBlockSize, oldOutputBlockSize ) )
                    throw new RuntimeException( "Cannot overwrite existing dataset if the block sizes are not the same." );
            }
        }

        // derive input and output value range
        final double minInputValue, maxInputValue;
        if ( valueRangeOptional.isPresent() )
        {
            minInputValue = valueRangeOptional.get().getA();
            maxInputValue = valueRangeOptional.get().getB();
        }
        else
        {
            if ( inputDataType == DataType.FLOAT32 || inputDataType == DataType.FLOAT64 )
            {
                minInputValue = 0;
                maxInputValue = 1;
            }
            else
            {
                final I inputType = N5Utils.type( inputDataType );
                minInputValue = inputType.getMinValue();
                maxInputValue = inputType.getMaxValue();
            }
        }

        final double minOutputValue, maxOutputValue;
        if ( outputDataType == DataType.FLOAT32 || outputDataType == DataType.FLOAT64 )
        {
            minOutputValue = 0;
            maxOutputValue = 1;
        }
        else
        {
            final O outputType = N5Utils.type( outputDataType );
            minOutputValue = outputType.getMinValue();
            maxOutputValue = outputType.getMaxValue();
        }

        System.out.println( "Input value range: " + Arrays.toString( new double[] { minInputValue, maxInputValue } ) );
        System.out.println( "Output value range: " + Arrays.toString( new double[] { minOutputValue, maxOutputValue } ) );

        final int dim = inputAttributes.getNumDimensions();

        if ( dim != downsamplingFactors.length )
            throw new IllegalArgumentException( "Downsampling parameters do not match data dimensionality." );

        final long[] outputDimensions = new long[ dim ];
        for ( int d = 0; d < dim; ++d )
            outputDimensions[ d ] = inputAttributes.getDimensions()[ d ] / downsamplingFactors[ d ];

        if ( Arrays.stream( outputDimensions ).min().getAsLong() < 1 )
            throw new IllegalArgumentException( "Degenerate output dimensions: " + Arrays.toString( outputDimensions ) );

        n5Output.createDataset(
                outputDatasetPath,
                outputDimensions,
                outputBlockSize,
                outputDataType,
                outputCompression
        );

        // set the downsampling factors attribute
        final int[] inputAbsoluteDownsamplingFactors = n5Input.getAttribute( inputDatasetPath, DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY, int[].class );
        final int[] outputAbsoluteDownsamplingFactors = new int[ downsamplingFactors.length ];
        for ( int d = 0; d < downsamplingFactors.length; ++d )
            outputAbsoluteDownsamplingFactors[ d ] = downsamplingFactors[ d ] * ( inputAbsoluteDownsamplingFactors != null ? inputAbsoluteDownsamplingFactors[ d ] : 1 );
        n5Output.setAttribute( outputDatasetPath, DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY, outputAbsoluteDownsamplingFactors );

        final CellGrid outputCellGrid = new CellGrid( outputDimensions, outputBlockSize );
        final long numDownsampledBlocks = Intervals.numElements( outputCellGrid.getGridDimensions() );
        final List< Long > blockIndexes = LongStream.range( 0, numDownsampledBlocks ).boxed().collect( Collectors.toList() );

        try {
            Files.delete(Paths.get(outputDatasetPath));
        } catch (NoSuchFileException x) {
            //Do nothing
        } catch (DirectoryNotEmptyException x) {
            System.err.format("%s not empty%n", outputDatasetPath);
        } catch (IOException x) {
            // File permission problems are caught here.
            System.err.println(x);
        }

        //System.out.println( "dim: " + outputDimensions[0] + " " + outputDimensions[1] + " "+ outputDimensions[2]);

        List<List<VVDBlockMetadata>> result = sparkContext.parallelize( blockIndexes, Math.min( blockIndexes.size(), MAX_PARTITIONS ) ).map( blockIndex ->
        {
            final CellGrid cellGrid = new CellGrid( outputDimensions, outputBlockSize );
            final long[] blockGridPosition = new long[ cellGrid.numDimensions() ];
            cellGrid.getCellGridPositionFlat( blockIndex, blockGridPosition );

            final long[] sourceMin = new long[ dim ], sourceMax = new long[ dim ], targetMin = new long[ dim ], targetMax = new long[ dim ];
            final int[] cellDimensions = new int[ dim ];
            cellGrid.getCellDimensions( blockGridPosition, targetMin, cellDimensions );
            for ( int d = 0; d < dim; ++d )
            {
                targetMax[ d ] = targetMin[ d ] + cellDimensions[ d ] - 1;
                sourceMin[ d ] = targetMin[ d ] * downsamplingFactors[ d ];
                sourceMax[ d ] = targetMax[ d ] * downsamplingFactors[ d ] + downsamplingFactors[ d ] - 1;
            }
            final Interval sourceInterval = new FinalInterval( sourceMin, sourceMax );
            final Interval targetInterval = new FinalInterval( targetMin, targetMax );

            final RandomAccessibleInterval< I > source = N5Utils.open( n5InputSupplier.get(), inputDatasetPath );
            final RandomAccessibleInterval< I > sourceBlock = Views.offsetInterval( source, sourceInterval );

            /* test if empty */
            final I defaultValue = Util.getTypeFromInterval( sourceBlock ).createVariable();
            boolean isEmpty = true;
            for ( final I t : Views.iterable( sourceBlock ) )
            {
                isEmpty &= defaultValue.valueEquals( t );
                if ( !isEmpty ) break;
            }
            if ( isEmpty )
                return new ArrayList<VVDBlockMetadata>();

            /* do if not empty */
            final RandomAccessibleInterval< I > targetBlock = new ArrayImgFactory<>( defaultValue ).create( targetInterval );
            Downsample.downsample( sourceBlock, targetBlock, downsamplingFactors );

            final O outputType = N5Utils.type( outputDataType );
            final RandomAccessible< O > convertedSource;
            if ( inputDataType == outputDataType )
            {
                convertedSource = ( RandomAccessible< O > ) targetBlock;
            }
            else
            {
                convertedSource = Converters.convert( targetBlock, new ClampingConverter<>(
                        minInputValue, maxInputValue,
                        minOutputValue, maxOutputValue
                ), outputType.createVariable() );
            }
            final RandomAccessibleInterval< O > convertedSourceInterval = Views.interval( convertedSource, targetBlock);

            if ( overwriteExisting)
            {
                // Empty blocks will not be written out. Delete blocks to avoid remnant blocks if overwriting.
                N5Utils.deleteBlock( convertedSourceInterval, n5OutputSupplier.get(), outputDatasetPath, blockGridPosition );
            }

            final long[] grid_dim = new long[ dim ];
            cellGrid.gridDimensions(grid_dim);
            return saveNonEmptyBlock( convertedSourceInterval, outputDatasetPath, n5OutputSupplier.get().getDatasetAttributes(outputDatasetPath) , blockGridPosition, grid_dim, outputType.createVariable() );

        } ).collect();

        ArrayList<VVDBlockMetadata> final_res = new ArrayList<VVDBlockMetadata>();
        for(List<VVDBlockMetadata> ls : result) {
            for(VVDBlockMetadata d : ls) {
                final_res.add(d);
            }
        }

        final_res.sort( comparing(VVDBlockMetadata::getFileOffset) );
        /*
        for(int i = 0; i < final_res.size()-1; i++) {
            final_res.get(i).setDataSize(final_res.get(i+1).getFileOffset() - final_res.get(i).getFileOffset());
        }
        try (final LockedFileChannel lockedChannel = LockedFileChannel.openForAppend(Paths.get(outputDatasetPath))) {
            long fsize = lockedChannel.getFileChannel().position();
            final_res.get(final_res.size()-1).setDataSize(fsize - final_res.get(final_res.size()-1).getFileOffset());
        }
        */
        for(VVDBlockMetadata d : final_res) {
            System.out.println( d.getVVDXMLBrickTag() );
            System.out.println( d.getVVDXMLFileTag() );
        }

        return final_res;
    }

    public static < T extends RealType< T > > void downsampleFunction( final RandomAccessible< T > input, final RandomAccessibleInterval< T > output, final int[] factor )
    {
        assert input.numDimensions() == output.numDimensions();
        assert input.numDimensions() == factor.length;

        final int n = input.numDimensions();
        final RectangleNeighborhoodFactory< T > f = RectangleNeighborhoodUnsafe.< T >factory();
        final long[] dim = new long[ n ];
        for ( int d = 0; d < n; ++d )
            dim[ d ] = factor[ d ];
        final Interval spanInterval = new FinalInterval( dim );

        final long[] minRequiredInput = new long[ n ];
        final long[] maxRequiredInput = new long[ n ];
        output.min( minRequiredInput );
        output.max( maxRequiredInput );
        for ( int d = 0; d < n; ++d )
        {
            minRequiredInput[ d ] *= factor[ d ];
            maxRequiredInput[ d ] *= factor[ d ];
            maxRequiredInput[ d ] += factor[ d ] - 1;
        }
        final RandomAccessibleInterval< T > requiredInput = Views.interval( input, new FinalInterval( minRequiredInput, maxRequiredInput ) );

        final RectangleShape.NeighborhoodsAccessible< T > neighborhoods = new RectangleShape.NeighborhoodsAccessible<>( requiredInput, spanInterval, f );
        final RandomAccess< Neighborhood< T > > block = neighborhoods.randomAccess();

        long size = 1;
        for ( int d = 0; d < n; ++d )
            size *= factor[ d ];
        final double scale = 1.0 / size;

        final Cursor< T > out = Views.iterable( output ).localizingCursor();
        while( out.hasNext() )
        {
            final T o = out.next();
            for ( int d = 0; d < n; ++d )
                block.setPosition( out.getLongPosition( d ) * factor[ d ], d );
            double sum = 0;
            for ( final T i : block.get() )
                sum += i.getRealDouble();
            o.setReal( sum * scale );
        }
    }

    public static void main( final String... args ) throws IOException, CmdLineException
    {
        final N5ToVVDSpark.Arguments parsedArgs = new N5ToVVDSpark.Arguments( args );

        ArrayList<List<VVDBlockMetadata>> vvdxml = new ArrayList<List<VVDBlockMetadata>>();

        final String outputDatasetPath = parsedArgs.getOutputDatasetPath();
        final int[][] downsamplingFactors = parsedArgs.getDownsamplingFactors();

        int bit_depth = 8;
        final int[] outputBlockSize;
        final Compression outputCompression;
        List<String> res_strs = new ArrayList<String>();

        try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
                .setAppName( "N5DownsamplerSpark" )
                .set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
        ) )
        {
            final N5ReaderSupplier n5InputSupplier = () -> new N5FSReader( parsedArgs.getInputN5Path() );
            final N5WriterSupplier n5OutputSupplier = () -> new N5FSWriter( parsedArgs.getOutputN5Path() );

            //if ( outputDatasetPath.length != downsamplingFactors.length )
            //    throw new IllegalArgumentException( "Number of output datasets does not match downsampling factors!" );

            for (int[] dfs : downsamplingFactors)
            {
                String str = "";
                for (int df : dfs)
                {
                    str += df;
                }
                System.out.println( "downsamplingFactors: " + str );
            }

            final N5Reader n5Input = n5InputSupplier.get();
            final DatasetAttributes inputAttributes = n5Input.getDatasetAttributes( parsedArgs.getInputDatasetPath() );

            final int[] inputBlockSize = inputAttributes.getBlockSize();
            final Compression inputCompression = inputAttributes.getCompression();
            final DataType inputDataType = inputAttributes.getDataType();
            outputBlockSize = parsedArgs.getBlockSize() != null ? parsedArgs.getBlockSize() : inputBlockSize;
            outputCompression = parsedArgs.getCompression() != null ? parsedArgs.getCompression() : inputCompression;
            final DataType outputDataType = parsedArgs.getDataType() != null ? parsedArgs.getDataType() : inputDataType;

            switch (outputDataType) {
                case UINT8:
                case INT8:
                    bit_depth = 8;
                    break;
                case UINT16:
                case INT16:
                    bit_depth = 16;
                    break;
                case UINT32:
                case INT32:
                    bit_depth = 32;
                    break;
                case UINT64:
                case INT64:
                    bit_depth = 64;
                    break;
                case FLOAT32:
                    bit_depth = 32;
                    break;
                case FLOAT64:
                    bit_depth = 64;
                    break;
                default:
                    throw new IllegalArgumentException("Type " + outputDataType.name() + " not supported!");
            }

            final Map<String, Class<?>> metaMap = n5Input.listAttributes(parsedArgs.getInputDatasetPath());
            double[] pixelResolution = new double[]{1.0, 1.0, 1.0};
            if (metaMap.containsKey(PIXEL_RESOLUTION_ATTRIBUTE_KEY)) {
                System.out.println(metaMap.get(PIXEL_RESOLUTION_ATTRIBUTE_KEY));
                if (metaMap.get(PIXEL_RESOLUTION_ATTRIBUTE_KEY) == double[].class)
                    pixelResolution = n5Input.getAttribute(parsedArgs.getInputDatasetPath(), PIXEL_RESOLUTION_ATTRIBUTE_KEY, double[].class);
                else if (metaMap.get(PIXEL_RESOLUTION_ATTRIBUTE_KEY) == Object.class) {
                    FinalVoxelDimensions fvd = n5Input.getAttribute( parsedArgs.getInputDatasetPath(), PIXEL_RESOLUTION_ATTRIBUTE_KEY, FinalVoxelDimensions.class );
                    if (fvd != null) {
                        for (int i = 0; i < fvd.numDimensions() && i < 3; i++)
                            pixelResolution[i] = fvd.dimension(i);
                    }
                }
            }
            res_strs.add(String.format("xspc=\"%f\" yspc=\"%f\" zspc=\"%f\"", pixelResolution[0], pixelResolution[1], pixelResolution[2]));

            vvdxml.add(downsample(
                    sparkContext,
                    n5InputSupplier,
                    parsedArgs.getInputDatasetPath(),
                    n5OutputSupplier,
                    outputDatasetPath + File.separator + String.format("vvd_Lv%d_Ch0_Fr0_data0", 0),
                    downsamplingFactors[ 0 ],
                    Optional.ofNullable(parsedArgs.getBlockSize()),
                    Optional.ofNullable(parsedArgs.getCompression()),
                    Optional.ofNullable(parsedArgs.getDataType()),
                    Optional.ofNullable(parsedArgs.getValueRange()),
                    true
            ));

            for ( int i = 1; i < downsamplingFactors.length; i++ )
            {
                res_strs.add(String.format("xspc=\"%f\" yspc=\"%f\" zspc=\"%f\"",
                        pixelResolution[0]*downsamplingFactors[i][0], pixelResolution[1]*downsamplingFactors[i][1], pixelResolution[2]*downsamplingFactors[i][2]));

                vvdxml.add(downsample(
                        sparkContext,
                        n5InputSupplier,
                        parsedArgs.getInputDatasetPath(),
                        n5OutputSupplier,
                        outputDatasetPath + File.separator + String.format("vvd_Lv%d_Ch0_Fr0_data0", i),
                        downsamplingFactors[ i ],
                        Optional.ofNullable(parsedArgs.getBlockSize()),
                        Optional.ofNullable(parsedArgs.getCompression()),
                        Optional.ofNullable(parsedArgs.getDataType()),
                        Optional.ofNullable(parsedArgs.getValueRange()),
                        true
                ));
            }
        }

        final Path xmlpath = Paths.get(outputDatasetPath, Paths.get(outputDatasetPath).getFileName() + ".vvd");
        System.out.println( xmlpath.toString() );

        try (BufferedWriter writer = Files.newBufferedWriter(xmlpath)) {
            writer.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            writer.newLine();
            writer.append(String.format("<BRK nChannel=\"1\" nFrame=\"1\" nLevel=\"%d\" MaskLv=\"%d\"  version=\"1.0\">", downsamplingFactors.length, downsamplingFactors.length));
            writer.newLine();

            for (int lv = 0; lv < downsamplingFactors.length; lv++) {
                long[] dims = vvdxml.get(lv).get(0).getDimension();
                writer.append(String.format("<Level lv=\"%d\" imageW=\"%d\" imageH=\"%d\" imageD=\"%d\" %s bitDepth=\"%d\" FileType=\"%s\">",
                        lv, dims[0], dims[1], dims[2], res_strs.get(lv), bit_depth, outputCompression.getType()));
                writer.newLine();

                writer.append(String.format("<Bricks brick_baseW=\"%d\" brick_baseH=\"%d\" brick_baseD=\"%d\">",
                        outputBlockSize[0], outputBlockSize[1], outputBlockSize[2]));
                writer.newLine();
                for (VVDBlockMetadata vbm : vvdxml.get(lv)) {
                    writer.append(vbm.getVVDXMLBrickTag());
                    writer.newLine();
                }
                writer.append("</Bricks>");
                writer.newLine();

                writer.append("<Files>");
                writer.newLine();
                for (VVDBlockMetadata vbm : vvdxml.get(lv)) {
                    vbm.setFileOffset(vbm.getFileOffset()+16L);
                    writer.append(vbm.getVVDXMLFileTag());
                    writer.newLine();
                }
                writer.append("</Files>");
                writer.newLine();

                writer.append("</Level>");
                writer.newLine();
            }
            writer.append("</BRK>");

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println( "Done" );
    }

    private static class Arguments implements Serializable
    {
        private static final long serialVersionUID = -1467734459169624759L;

        @Option(name = "-ni", aliases = { "-n", "--inputN5Path" }, required = true,
                usage = "Path to the input N5 container.")
        private String n5InputPath;

        @Option(name = "-i", aliases = { "--inputDatasetPath" }, required = true,
                usage = "Path to the input dataset within the N5 container (e.g. data/group/s0).")
        private String inputDatasetPath;

        @Option(name = "-no", aliases = { "--outputN5Path" }, required = false,
                usage = "Path to the output N5 container (by default the output dataset is stored within the same container as the input dataset).")
        private String n5OutputPath;

        @Option(name = "-o", aliases = { "--outputDatasetPath" }, required = true,
                usage = "Path(s) to the output dataset to be created (e.g. data/group/s1).")
        private String outputDatasetPath;

        @Option(name = "-f", aliases = { "--factors" }, required = true, handler = StringArrayOptionHandler.class,
                usage = "Downsampling factors. If using multiple, each factor builds on the last.")
        private String[] downsamplingFactors;

        @Option(name = "-b", aliases = { "--blockSize" }, required = false,
                usage = "Block size for the output dataset (by default the same block size is used as for the input dataset).")
        private String blockSizeStr;

        @Option(name = "-c", aliases = { "--compression" }, required = false,
                usage = "Compression to be used for the converted dataset (by default the same compression is used as for the input dataset).")
        private N5Compression n5Compression;

        @Option(name = "-t", aliases = { "--type" }, required = false,
                usage = "Type to be used for the converted dataset (by default the same type is used as for the input dataset)."
                        + "If a different type is used, the values are mapped to the range of the output type, rounding to the nearest integer value if necessary.")
        private DataType dataType;

        @Option(name = "-min", aliases = { "--minValue" }, required = false,
                usage = "Minimum value of the input range to be used for the conversion (default is min type value for integer types, or 0 for real types).")
        private Double minValue;

        @Option(name = "-max", aliases = { "--maxValue" }, required = false,
                usage = "Maximum value of the input range to be used for the conversion (default is max type value for integer types, or 1 for real types).")
        private Double maxValue;

        private int[] blockSize;
        private boolean parsedSuccessfully = false;

        public Arguments( final String... args ) throws IllegalArgumentException
        {
            final CmdLineParser parser = new CmdLineParser( this );
            try
            {
                for (String ss : args)
                    System.out.println( ss );

                parser.parseArgument( args );

                blockSize = blockSizeStr != null ? CmdUtils.parseIntArray( blockSizeStr ) : null;

                if ( Objects.isNull( minValue ) != Objects.isNull( maxValue ) )
                    throw new IllegalArgumentException( "minValue and maxValue should be either both specified or omitted." );

                n5OutputPath = outputDatasetPath;

                parsedSuccessfully = true;
            }
            catch ( final CmdLineException e )
            {
                System.err.println( e.getMessage() );
                parser.printUsage( System.err );
                System.exit( 1 );
            }
        }

        public String getInputN5Path() { return n5InputPath; }
        public String getOutputN5Path() { return n5OutputPath != null ? n5OutputPath : n5InputPath; }
        public String getInputDatasetPath() { return inputDatasetPath; }
        public String getOutputDatasetPath() { return outputDatasetPath; }
        public int[] getBlockSize() { return blockSize; }
        public Compression getCompression() { return n5Compression != null ? n5Compression.get() : null; }
        public DataType getDataType() { return dataType; }
        public Pair< Double, Double > getValueRange() { return Objects.nonNull( minValue ) && Objects.nonNull( maxValue ) ? new ValuePair<>( minValue, maxValue ) : null; }
        public int[][] getDownsamplingFactors() { return CmdUtils.parseMultipleIntArrays( downsamplingFactors ); }
    }

    static class VVDBlockMetadata implements Serializable
    {
        private long m_brickID;
        private long m_dataSize;
        private String m_filePath;
        private String m_compression;
        private long m_fileOffset;
        private long[] m_blockSize;
        private long[] m_boundMin;
        private long[] m_dimension;

        public VVDBlockMetadata(long brickID)
        {
            m_brickID = brickID;
            m_dataSize = 0;
            m_filePath = "";
            m_compression = "";
            m_fileOffset = 0;
            m_blockSize = new long[]{0L, 0L, 0L};
            m_boundMin = new long[]{0L, 0L, 0L};
            m_dimension = new long[]{-1L, -1L, -1L};
        }

        public VVDBlockMetadata(long brickID, long dataSize, String filePath, String compression, long fileOffset, long[] blockSize, long[] boundMin, long[] dimension)
        {
            m_brickID = brickID;
            m_dataSize = dataSize;
            m_filePath = filePath;
            m_compression = compression;
            m_fileOffset = fileOffset;
            m_blockSize = blockSize;
            m_boundMin = boundMin;
            m_dimension = dimension;
        }

        public long getBrickID() { return m_brickID; }
        public long getDataSize() { return m_dataSize; }
        public void setDataSize(long dataSize) { m_dataSize = dataSize; }
        public String getFilePath() { return m_filePath; }
        public String getCompression() { return m_compression; }
        public long getFileOffset() { return m_fileOffset; }
        public void setFileOffset(long fileOffset) { m_fileOffset = fileOffset; }
        public long[] getBlockSize() { return m_blockSize; }
        public long[] getBoundMin() { return m_boundMin; }
        public long[] getDimension() { return m_dimension; }
        public String getVVDXMLFileTag()
        {
            return String.format("<File brickID=\"%d\" channel=\"0\" datasize=\"%d\" filename=\"%s\" filetype=\"%s\" frame=\"0\" offset=\"%d\"/>", m_brickID, m_dataSize, m_filePath, m_compression, m_fileOffset);
        }
        public String getVVDXMLBrickTag()
        {
            return String.format("<Brick id=\"%d\" width=\"%d\" height=\"%d\" depth=\"%d\" st_x=\"%d\" st_y=\"%d\" st_z=\"%d\">\n", m_brickID, m_blockSize[0], m_blockSize[1], m_blockSize[2], m_boundMin[0], m_boundMin[1], m_boundMin[2])
                    + "<tbox x0=\"0.0\" x1=\"1.0\" y0=\"0.0\" y1=\"1.0\" z0=\"0.0\" z1=\"1.0\"/>\n"
                    + String.format("<bbox x0=\"%f\" x1=\"%f\" y0=\"%f\" y1=\"%f\" z0=\"%f\" z1=\"%f\"/>\n",
                    (double)m_boundMin[0] / (double)m_dimension[0], (double)(m_boundMin[0]+m_blockSize[0]) / (double)m_dimension[0],
                    (double)m_boundMin[1] / (double)m_dimension[1], (double)(m_boundMin[1]+m_blockSize[1]) / (double)m_dimension[1],
                    (double)m_boundMin[2] / (double)m_dimension[2], (double)(m_boundMin[2]+m_blockSize[2]) / (double)m_dimension[2])
                    + "</Brick>";
        }
    }

    ///////////////////////////////////////////////
    static class ClampingConverter< I extends NativeType< I > & RealType< I >, O extends NativeType< O > & RealType< O > > implements Converter< I, O >
    {
        private final double minInputValue, maxInputValue;
        private final double minOutputValue, maxOutputValue;
        private final double inputValueRange, outputValueRange;

        public ClampingConverter(
                final double minInputValue, final double maxInputValue,
                final double minOutputValue, final double maxOutputValue )
        {
            this.minInputValue = minInputValue; this.maxInputValue = maxInputValue;
            this.minOutputValue = minOutputValue; this.maxOutputValue = maxOutputValue;

            inputValueRange = maxInputValue - minInputValue;
            outputValueRange = maxOutputValue - minOutputValue;
        }

        @Override
        public void convert( final I input, final O output )
        {
            final double inputValue = input.getRealDouble();
            if ( inputValue <= minInputValue )
            {
                output.setReal( minOutputValue );
            }
            else if ( inputValue >= maxInputValue )
            {
                output.setReal( maxOutputValue );
            }
            else
            {
                final double normalizedInputValue = ( inputValue - minInputValue ) / inputValueRange;
                final double realOutputValue = normalizedInputValue * outputValueRange + minOutputValue;
                output.setReal( realOutputValue );
            }
        }
    }

    static void cropBlockDimensions(
            final long[] max,
            final long[] offset,
            final long[] gridOffset,
            final int[] blockDimensions,
            final long[] croppedBlockDimensions,
            final int[] intCroppedBlockDimensions,
            final long[] gridPosition) {

        for (int d = 0; d < max.length; ++d) {
            croppedBlockDimensions[d] = Math.min(blockDimensions[d], max[d] - offset[d] + 1);
            intCroppedBlockDimensions[d] = (int)croppedBlockDimensions[d];
            gridPosition[d] = offset[d] / blockDimensions[d] + gridOffset[d];
        }
    }

    @SuppressWarnings("unchecked")
    private static final <T extends Type<T>> DataBlock<?> createNonEmptyDataBlock(
            final RandomAccessibleInterval<?> source,
            final DataType dataType,
            final int[] intBlockSize,
            final long[] longBlockSize,
            final long[] gridPosition,
            final T defaultValue) {

        final DataBlock<?> dataBlock = dataType.createDataBlock(intBlockSize, gridPosition);
        final boolean isEmpty;
        switch (dataType) {
            case UINT8:
                isEmpty = N5CellLoader.burnInTestAllEqual(
                        (RandomAccessibleInterval<UnsignedByteType>)source,
                        ArrayImgs.unsignedBytes((byte[])dataBlock.getData(), longBlockSize),
                        (UnsignedByteType)defaultValue);
                break;
            case INT8:
                isEmpty = N5CellLoader.burnInTestAllEqual(
                        (RandomAccessibleInterval<ByteType>)source,
                        ArrayImgs.bytes((byte[])dataBlock.getData(), longBlockSize),
                        (ByteType)defaultValue);
                break;
            case UINT16:
                isEmpty = N5CellLoader.burnInTestAllEqual(
                        (RandomAccessibleInterval<UnsignedShortType>)source,
                        ArrayImgs.unsignedShorts((short[])dataBlock.getData(), longBlockSize),
                        (UnsignedShortType)defaultValue);
                break;
            case INT16:
                isEmpty = N5CellLoader.burnInTestAllEqual(
                        (RandomAccessibleInterval<ShortType>)source,
                        ArrayImgs.shorts((short[])dataBlock.getData(), longBlockSize),
                        (ShortType)defaultValue);
                break;
            case UINT32:
                isEmpty = N5CellLoader.burnInTestAllEqual(
                        (RandomAccessibleInterval<UnsignedIntType>)source,
                        ArrayImgs.unsignedInts((int[])dataBlock.getData(), longBlockSize),
                        (UnsignedIntType)defaultValue);
                break;
            case INT32:
                isEmpty = N5CellLoader.burnInTestAllEqual(
                        (RandomAccessibleInterval<IntType>)source,
                        ArrayImgs.ints((int[])dataBlock.getData(), longBlockSize),
                        (IntType)defaultValue);
                break;
            case UINT64:
                isEmpty = N5CellLoader.burnInTestAllEqual(
                        (RandomAccessibleInterval<UnsignedLongType>)source,
                        ArrayImgs.unsignedLongs((long[])dataBlock.getData(), longBlockSize),
                        (UnsignedLongType)defaultValue);
                break;
            case INT64:
                isEmpty = N5CellLoader.burnInTestAllEqual(
                        (RandomAccessibleInterval<LongType>)source,
                        ArrayImgs.longs((long[])dataBlock.getData(), longBlockSize),
                        (LongType)defaultValue);
                break;
            case FLOAT32:
                isEmpty = N5CellLoader.burnInTestAllEqual(
                        (RandomAccessibleInterval<FloatType>)source,
                        ArrayImgs.floats((float[])dataBlock.getData(), longBlockSize),
                        (FloatType)defaultValue);
                break;
            case FLOAT64:
                isEmpty = N5CellLoader.burnInTestAllEqual(
                        (RandomAccessibleInterval<DoubleType>)source,
                        ArrayImgs.doubles((double[])dataBlock.getData(), longBlockSize),
                        (DoubleType)defaultValue);
                break;
            default:
                throw new IllegalArgumentException("Type " + dataType.name() + " not supported!");
        }

        return isEmpty ? null : dataBlock;
    }

    public static final <T extends NativeType<T>> List<VVDBlockMetadata> saveNonEmptyBlock(
            RandomAccessibleInterval<T> source,
            final String filepath,
            final DatasetAttributes attributes,
            final long[] gridOffset,
            final long[] gridDims,
            final T defaultValue) throws IOException {

        ArrayList<VVDBlockMetadata> ret = new ArrayList<VVDBlockMetadata>();
        source = Views.zeroMin(source);
        final int n = source.numDimensions();
        final long[] max = Intervals.maxAsLongArray(source);
        final long[] offset = new long[n];
        final long[] gridPosition = new long[n];
        final int[] blockSize = attributes.getBlockSize();
        final long[] dimension = attributes.getDimensions();
        final int[] intCroppedBlockSize = new int[n];
        final long[] longCroppedBlockSize = new long[n];

        String compression = attributes.getCompression().getType();
        if (compression == null) {
            compression = "RAW";
        }

        for (int d = 0; d < n;) {
            cropBlockDimensions(
                    max,
                    offset,
                    gridOffset,
                    blockSize,
                    longCroppedBlockSize,
                    intCroppedBlockSize,
                    gridPosition);
            final RandomAccessibleInterval<T> sourceBlock = Views.offsetInterval(source, offset, longCroppedBlockSize);
            final DataBlock<?> dataBlock = createNonEmptyDataBlock(
                    sourceBlock,
                    attributes.getDataType(),
                    intCroppedBlockSize,
                    longCroppedBlockSize,
                    gridPosition,
                    defaultValue);

            if (dataBlock != null) {
                long brickID = 0;
                for (int dd = 0; dd < n; dd++) {
                    long pitch = 1;
                    for (int ddd = dd - 1; ddd >= 0; ddd--) {
                        pitch *= gridDims[ddd];
                    }
                    brickID += gridPosition[dd] * pitch;
                }
                final Path path = Paths.get(filepath + "_ID" + brickID);
                Files.createDirectories(path.getParent());
                try (final LockedFileChannel lockedChannel = LockedFileChannel.openForWriting(path)/*LockedFileChannel.openForAppend(path)*/) {
                    final long file_offset = lockedChannel.getFileChannel().position();
                    final OutputStream ostream = Channels.newOutputStream(lockedChannel.getFileChannel());
                    DefaultBlockWriter.writeBlock(ostream, attributes, dataBlock);
                    final long data_size = 0L;//lockedChannel.getFileChannel().position() - file_offset;
                    final long[] bound_min = {gridPosition[0] * blockSize[0], gridPosition[1] * blockSize[1], gridPosition[2] * blockSize[2]};
                    ret.add(new VVDBlockMetadata(brickID, data_size, path.getFileName().toString(), compression, file_offset, longCroppedBlockSize, bound_min, dimension));
                    System.out.println( "saveNonEmptyBlock: finished " + brickID );
                }
            }

            for (d = 0; d < n; ++d) {
                offset[d] += blockSize[d];
                if (offset[d] <= max[d])
                    break;
                else
                    offset[d] = 0;
            }
        }
        return ret;
    }
/*
    public static final <T extends NativeType<T>> String saveNonEmptyBlock(
            final RandomAccessibleInterval<T> source,
            final String filepath,
            final DatasetAttributes attributes,
            final T defaultValue) throws IOException {

        final int[] blockSize = attributes.getBlockSize();
        final long[] gridOffset = new long[blockSize.length];
        Arrays.setAll(gridOffset, d -> source.min(d) / blockSize[d]);
        return saveNonEmptyBlock(source, filepath, attributes, gridOffset, defaultValue);
    }
*/
}

