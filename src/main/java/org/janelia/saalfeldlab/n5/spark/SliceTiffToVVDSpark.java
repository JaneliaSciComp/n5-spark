package org.janelia.saalfeldlab.n5.spark;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.Collator;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.supplier.N5WriterSupplier;
import org.janelia.saalfeldlab.n5.spark.util.CmdUtils;
import org.janelia.saalfeldlab.n5.spark.util.N5Compression;
import org.janelia.saalfeldlab.n5.spark.util.N5SparkUtils;
import org.janelia.saalfeldlab.n5.spark.util.TiffUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.esotericsoftware.kryo.Kryo;

import ij.ImagePlus;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.util.Grids;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.Views;
import scala.Tuple2;
import se.sawano.java.text.AlphanumericComparator;


import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import net.imglib2.view.*;
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

import net.imglib2.view.ExtendedRandomAccessibleInterval;

import static java.util.Comparator.*;

import org.janelia.saalfeldlab.n5.spark.N5ToVVDSpark;


public class SliceTiffToVVDSpark {
    private static final int MAX_PARTITIONS = 15000;

    public static final String DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY = "downsamplingFactors";
    public static final String PIXEL_RESOLUTION_ATTRIBUTE_KEY = "pixelResolution";

    public static final String TEMP_N5_DIR = "temp_n5_dir_tif";

    public static < T extends NativeType< T > > void createTempN5DatasetFromTiff(
            final JavaSparkContext sparkContext,
            final String inputDirPath,
            final N5WriterSupplier n5OutputSupplier,
            final String tempDatasetPath,
            final int[] blockSize,
            final Compression compression) throws IOException
    {
        final TiffUtils.TiffReader tiffReader = new TiffUtils.FileTiffReader();
        final List< String > tiffSliceFilepaths = Files.walk( Paths.get( inputDirPath ) )
                .filter( p -> p.toString().toLowerCase().endsWith( ".tif" ) || p.toString().toLowerCase().endsWith( ".tiff" ) )
                .map( p -> p.toString() )
                .sorted( new AlphanumericComparator( Collator.getInstance() ) )
                .collect( Collectors.toList() );

        if ( tiffSliceFilepaths.isEmpty() )
            throw new RuntimeException( "Specified input directory does not contain any TIFF slices" );

        // open the first image in the series to find out the size of the dataset and its data type
        final long[] inputDimensions;
        final DataType inputDataType;
        {
            final ImagePlus imp = tiffReader.openTiff( tiffSliceFilepaths.iterator().next() );
            final RandomAccessibleInterval< T > img = ( RandomAccessibleInterval< T > ) ImagePlusImgs.from( imp );
            if ( img.numDimensions() != 2 )
                throw new RuntimeException( "TIFF images in the specified directory are not 2D" );

            inputDimensions = new long[] { img.dimension( 0 ), img.dimension( 1 ), tiffSliceFilepaths.size() };
            inputDataType = N5Utils.dataType( Util.getTypeFromInterval( img ) );
        }

        final N5Writer n5TempOutput = n5OutputSupplier.get();
        final int[] tmpBlockSize = new int[ 3 ];
        tmpBlockSize[ 2 ] = 1;
        for ( int d = 0; d < 2; ++d )
            tmpBlockSize[ d ] = blockSize[ d ] * Math.max( ( int ) Math.round( Math.sqrt( blockSize[ 2 ] ) ), 1 );

        try {
            N5RemoveSpark.remove(sparkContext, n5OutputSupplier, tempDatasetPath);
        } catch (IOException e) {
            //Do nothing
        }

        // convert to temporary N5 dataset with block size = 1 in the slice dimension and increased block size in other dimensions
        n5TempOutput.createDataset( tempDatasetPath, inputDimensions, tmpBlockSize, inputDataType, compression );
        final List< Integer > sliceIndices = IntStream.range( 0, tiffSliceFilepaths.size() ).boxed().collect( Collectors.toList() );
        sparkContext.parallelize( sliceIndices, Math.min( sliceIndices.size(), MAX_PARTITIONS ) ).foreach( sliceIndex ->
                {
                    final ImagePlus imp = tiffReader.openTiff( tiffSliceFilepaths.get( sliceIndex ) );
                    final RandomAccessibleInterval< T > img = ( RandomAccessibleInterval< T > ) ImagePlusImgs.from( imp );
                    N5Utils.saveNonEmptyBlock(
                            Views.addDimension( img, 0, 0 ),
                            n5OutputSupplier.get(),
                            tempDatasetPath,
                            new long[] { 0, 0, sliceIndex },
                            Util.getTypeFromInterval( img ).createVariable()
                    );
                }
        );
    }

    public static void main( final String... args ) throws IOException, CmdLineException
    {
        final SliceTiffToVVDSpark.Arguments parsedArgs = new SliceTiffToVVDSpark.Arguments( args );

        ArrayList<List<N5ToVVDSpark.VVDBlockMetadata>> vvdxml = new ArrayList<List<N5ToVVDSpark.VVDBlockMetadata>>();

        final String inputDirPath = parsedArgs.getInputDirPath();
        final String outputDatasetPath = parsedArgs.getOutputDatasetPath();
        double[][] downsamplingFactors = parsedArgs.getDownsamplingFactors();
        double[] minDownsamplingFactors = parsedArgs.getMinDownsamplingFactors();
        double[] maxDownsamplingFactors = parsedArgs.getMaxDownsamplingFactors();
        Integer numLevels = parsedArgs.getNumLevels();

        int bit_depth = 8;
        final int[] outputBlockSize;
        final Compression outputCompression;
        List<String> res_strs = new ArrayList<String>();

        try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
                .setAppName( "SliceTifftoVVDSpark" )
                .set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
        ) )
        {
            final N5WriterSupplier n5OutputSupplier = () -> new N5FSWriter( parsedArgs.getOutputN5Path() );

            final String tmpDataset = TEMP_N5_DIR;
            createTempN5DatasetFromTiff(
                    sparkContext,
                    inputDirPath,
                    n5OutputSupplier,
                    tmpDataset,
                    parsedArgs.getBlockSize(),
                    parsedArgs.getCompression()
            );
            final N5Writer n5TempOutput = n5OutputSupplier.get();
            final DatasetAttributes inputAttributes = n5TempOutput.getDatasetAttributes( tmpDataset );
            final DataType inputDataType = inputAttributes.getDataType();
            final long[] inputDimensions = inputAttributes.getDimensions();

            if (downsamplingFactors != null) {
                for (double[] dfs : downsamplingFactors) {
                    String str = "";
                    for (double df : dfs) {
                        str += " " + df + ",";
                    }
                    System.out.println("downsamplingFactors:" + str);
                }
            }

            outputBlockSize = parsedArgs.getBlockSize();
            outputCompression = parsedArgs.getCompression();
            final DataType outputDataType = parsedArgs.getDataType() != null ? parsedArgs.getDataType() : inputDataType;

            if ( (numLevels != null && numLevels > 0) || minDownsamplingFactors != null || maxDownsamplingFactors != null || downsamplingFactors == null )
            {
                if (numLevels == null || numLevels <= 0)
                    numLevels = new Integer(5);
                if (minDownsamplingFactors == null) {
                    minDownsamplingFactors = new double[1];
                    minDownsamplingFactors[0] = 1.0;
                }
                if (maxDownsamplingFactors == null) {
                    maxDownsamplingFactors = new double[1];
                    maxDownsamplingFactors[0] = 10.0;
                }

                double[][] newDownsamplingFactors = new double[numLevels][];
                for (int i = 0; i < numLevels; i++) {
                    newDownsamplingFactors[i] = new double[inputDimensions.length];
                    for  (int d = 0; d < inputDimensions.length; d++) {
                        int minid = d;
                        int maxid = d;
                        if (d >= minDownsamplingFactors.length)
                            minid = minDownsamplingFactors.length - 1;
                        if (d >= maxDownsamplingFactors.length)
                            maxid = maxDownsamplingFactors.length - 1;
                        if (numLevels > 1)
                            newDownsamplingFactors[i][d] = minDownsamplingFactors[minid] + (maxDownsamplingFactors[maxid] - minDownsamplingFactors[minid]) / (numLevels-1) * i;
                        else
                            newDownsamplingFactors[i][d] = minDownsamplingFactors[minid];
                    }
                }
                downsamplingFactors = newDownsamplingFactors;
            }

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

            double[] pixelResolution = parsedArgs.getPixResolution() != null ? parsedArgs.getPixResolution(): new double[]{1.0, 1.0, 1.0};

            for ( int i = 0; i < downsamplingFactors.length; i++ )
            {
                final double[] adjustedDownsamplingFactor = new double[ inputDimensions.length ];
                double last = 1.0;
                for ( int d = 0; d < inputDimensions.length; ++d ) {
                    double dval = 1.0;
                    if (d < downsamplingFactors[i].length && downsamplingFactors[i][d] > 0.0) {
                        dval = downsamplingFactors[i][d];
                        last = dval;
                    }
                    else
                        dval = last;
                    adjustedDownsamplingFactor[d] = (double) inputDimensions[d] / (long) (inputDimensions[d] / dval + 0.5);
                }

                System.out.println("adjustedDownsamplingFactor:" + Arrays.toString(adjustedDownsamplingFactor));

                res_strs.add(String.format("xspc=\"%f\" yspc=\"%f\" zspc=\"%f\"",
                        pixelResolution[0]*adjustedDownsamplingFactor[0], pixelResolution[1]*adjustedDownsamplingFactor[1], pixelResolution[2]*adjustedDownsamplingFactor[2]));

                int[] blockSize = parsedArgs.getBlockSize();
                if (i == downsamplingFactors.length - 1) {
                    final int dim = inputDimensions.length;
                    blockSize = new int[ dim ];
                    for ( int d = 0; d < dim; ++d )
                        blockSize[d] = (int)(inputDimensions[d] / adjustedDownsamplingFactor[d] + 0.5);
                }

                vvdxml.add(N5ToVVDSpark.downsample(
                        sparkContext,
                        n5OutputSupplier,
                        tmpDataset,
                        n5OutputSupplier,
                        outputDatasetPath + File.separator + Paths.get(outputDatasetPath).getFileName() + String.format("_Lv%d_Ch0_Fr0_data0", i),
                        adjustedDownsamplingFactor,
                        Optional.ofNullable(blockSize),
                        Optional.ofNullable(parsedArgs.getCompression()),
                        Optional.ofNullable(parsedArgs.getDataType()),
                        Optional.ofNullable(parsedArgs.getValueRange()),
                        true
                ));
            }

            N5RemoveSpark.remove( sparkContext, n5OutputSupplier, tmpDataset );
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
                for (N5ToVVDSpark.VVDBlockMetadata vbm : vvdxml.get(lv)) {
                    writer.append(vbm.getVVDXMLBrickTag());
                    writer.newLine();
                }
                writer.append("</Bricks>");
                writer.newLine();

                writer.append("<Files>");
                writer.newLine();
                for (N5ToVVDSpark.VVDBlockMetadata vbm : vvdxml.get(lv)) {
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

        @Option(name = "-i", aliases = { "--inputDirPath" }, required = true,
                usage = "Path to an input directory containing TIFF slices")
        private String inputDirPath;

        @Option(name = "-no", aliases = { "--outputN5Path" }, required = false,
                usage = "Path to the output N5 container (by default the output dataset is stored within the same container as the input dataset).")
        private String n5OutputPath;

        @Option(name = "-o", aliases = { "--outputDatasetPath" }, required = true,
                usage = "Path(s) to the output dataset to be created (e.g. data/group/s1).")
        private String outputDatasetPath;

        @Option(name = "-f", aliases = { "--factors" }, required = false, handler = StringArrayOptionHandler.class,
                usage = "Downsampling factors. If using multiple, each factor builds on the last. This cannot be used with --min_factors, --max_factors and --levels")
        private String[] downsamplingFactors;

        @Option(name = "-fmin", aliases = { "--min_factors" }, required = false, handler = StringArrayOptionHandler.class,
                usage = "Minimum downsampling factors. Default value: 0")
        private String minDownsamplingFactorsStr;

        @Option(name = "-fmax", aliases = { "--max_factors" }, required = false, handler = StringArrayOptionHandler.class,
                usage = "Maximum downsampling factors. Default value: 10")
        private String maxDownsamplingFactorsStr;

        @Option(name = "-l", aliases = { "--levels" }, required = false,
                usage = "Number of levels in a resolution pyramid. Default value: 6")
        private Integer numLevels;

        @Option(name = "-b", aliases = { "--blockSize" }, required = true,
                usage = "Block size for the output dataset (by default the same block size is used as for the input dataset).")
        private String blockSizeStr;

        @Option(name = "-c", aliases = { "--compression" }, required = true,
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

        @Option(name = "-pr", aliases = { "--pix_res" }, required = false, handler = StringArrayOptionHandler.class,
                usage = "Pixel resolution. Default value: 1.0,1.0,1.0")
        private String pixResolutionStr;

        private int[] blockSize;
        private double[] minDownsamplingFactors;
        private double[] maxDownsamplingFactors;
        private double[] pixResolution;
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
                minDownsamplingFactors = minDownsamplingFactorsStr != null ? CmdUtils.parseDoubleArray( minDownsamplingFactorsStr ) : null;
                maxDownsamplingFactors = maxDownsamplingFactorsStr != null ? CmdUtils.parseDoubleArray( maxDownsamplingFactorsStr ) : null;
                pixResolution = pixResolutionStr != null ? CmdUtils.parseDoubleArray( pixResolutionStr ) : null;

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

        public String getInputDirPath() { return inputDirPath; }
        public String getOutputN5Path() { return n5OutputPath != null ? n5OutputPath : inputDirPath; }
        public String getOutputDatasetPath() { return outputDatasetPath; }
        public int[] getBlockSize() { return blockSize; }
        public Compression getCompression() { return n5Compression != null ? n5Compression.get() : null; }
        public DataType getDataType() { return dataType; }
        public Pair< Double, Double > getValueRange() { return Objects.nonNull( minValue ) && Objects.nonNull( maxValue ) ? new ValuePair<>( minValue, maxValue ) : null; }
        public double[][] getDownsamplingFactors() { return CmdUtils.parseMultipleDoubleArrays( downsamplingFactors ); }
        public double[] getMinDownsamplingFactors() { return minDownsamplingFactors; }
        public double[] getMaxDownsamplingFactors() { return maxDownsamplingFactors; }
        public Integer getNumLevels() { return numLevels; }
        public double[] getPixResolution() { return pixResolution; }
    }
}
