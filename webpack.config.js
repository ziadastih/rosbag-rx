const path = require("path");
const webpack = require("webpack");
const HtmlWebpackPlugin = require("html-webpack-plugin");
module.exports = {
  mode: "development",
  cache: false,
  entry: {
    bundle: path.resolve(__dirname, "src/dev/dev-index.ts"),
  },
  output: {
    filename: "[name][contenthash].js",
    path: path.resolve(__dirname, "public"),
    clean: true,
  },
  devServer: {
    liveReload: true,
    open: true,
    hot: true,
    compress: true,
    historyApiFallback: true,
  },
  devtool: "source-map",
  module: {
    rules: [
      {
        test: /\.ts$/,
        use: "ts-loader",
        exclude: /node_modules/,
        include: [path.resolve(__dirname, "src")],
      },
    ],
  },
  resolve: {
    extensions: [".ts", ".js"],
    fallback: {
      buffer: require.resolve("buffer/"),
    },
  },
  plugins: [
    new HtmlWebpackPlugin({
      title: "rosbag manager dev",
      filename: "index.html",
      template: "src/dev/template.html",
    }),
    new webpack.ProvidePlugin({
      Buffer: ["buffer", "Buffer"],
    }),
  ],
};
