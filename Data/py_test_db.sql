-- phpMyAdmin SQL Dump
-- version 5.2.0
-- https://www.phpmyadmin.net/
--
-- Host: 127.0.0.1
-- Generation Time: Apr 23, 2024 at 04:40 PM
-- Server version: 10.4.27-MariaDB
-- PHP Version: 8.2.0

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `py_test_db`
--

-- --------------------------------------------------------

--
-- Table structure for table `admins`
--

CREATE TABLE `admins` (
  `id` int(11) NOT NULL,
  `name` varchar(255) NOT NULL,
  `email` varchar(255) NOT NULL,
  `password` varchar(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `admins`
--

INSERT INTO `admins` (`id`, `name`, `email`, `password`) VALUES
(1, 'admin', 'admin@admin.com', '98d74e3d7ab10fbc19ba04dbb589b357748a3ac55f86fccc64a89340c928014f'),
(2, 'admin1', 'admin1@admin.com', '98d74e3d7ab10fbc19ba04dbb589b357748a3ac55f86fccc64a89340c928014f'),
(3, 'admin2', 'admin2@admin.com', '98d74e3d7ab10fbc19ba04dbb589b357748a3ac55f86fccc64a89340c928014f');

-- --------------------------------------------------------

--
-- Table structure for table `delays`
--

CREATE TABLE `delays` (
  `Car_ID` longtext NOT NULL,
  `Start_Gate` varchar(255) NOT NULL,
  `End_Gate` varchar(255) NOT NULL,
  `Start_Date` longtext NOT NULL,
  `Arrival_End_Date` longtext NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Table structure for table `drivers`
--

CREATE TABLE `drivers` (
  `ID` int(11) NOT NULL,
  `name` varchar(255) NOT NULL,
  `email` varchar(255) NOT NULL,
  `password` varchar(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `drivers`
--

INSERT INTO `drivers` (`ID`, `name`, `email`, `password`) VALUES
(1, 'Ezio Auditore', 'Ezio_Auditore@gmail.com', '98d74e3d7ab10fbc19ba04dbb589b357748a3ac55f86fccc64a89340c928014f'),
(2, 'Ali Ahmed', 'Ali_Ahmed@gmail.com', '98d74e3d7ab10fbc19ba04dbb589b357748a3ac55f86fccc64a89340c928014f'),
(3, 'Mohamed Abdo', 'Mohamed_Abdo@gmail.com', '98d74e3d7ab10fbc19ba04dbb589b357748a3ac55f86fccc64a89340c928014f'),
(4, 'Nour Hani', 'Nour_Hani@gmail.com', '98d74e3d7ab10fbc19ba04dbb589b357748a3ac55f86fccc64a89340c928014f');

-- --------------------------------------------------------

--
-- Table structure for table `travels`
--

CREATE TABLE `travels` (
  `Travel_id` varchar(255) NOT NULL,
  `Vehicle_id` int(11) NOT NULL,
  `Start_Gate` varchar(255) DEFAULT NULL,
  `End_Gate` varchar(255) DEFAULT NULL,
  `Distance` int(10) DEFAULT NULL,
  `Start_Travel_Date` longtext NOT NULL,
  `End_Travel_Date` longtext NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `travels`
--

INSERT INTO `travels` (`Travel_id`, `Vehicle_id`, `Start_Gate`, `End_Gate`, `Distance`, `Start_Travel_Date`, `End_Travel_Date`) VALUES
('3_Car', 1, 'Giza', 'Cairo', 7, '2024-03-11 15:34:01', '2024-03-11 15:34:01'),
('4_Taxi', 2, 'Giza', 'Cairo', 7, '2024-03-11 15:34:01', '2024-03-11 15:34:01');

-- --------------------------------------------------------

--
-- Table structure for table `vehicles`
--

CREATE TABLE `vehicles` (
  `id` int(11) NOT NULL,
  `driver_id` int(11) NOT NULL,
  `Number_Type` varchar(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `vehicles`
--

INSERT INTO `vehicles` (`id`, `driver_id`, `Number_Type`) VALUES
(1, 1, '3_Car'),
(2, 1, '4_Taxi'),
(3, 1, '1_Motorcycle'),
(4, 2, '4112_Car'),
(5, 2, '1423_Bus'),
(6, 3, '1110_Taxi'),
(7, 3, '4121_Car'),
(8, 4, '9812_Motorcycle'),
(9, 4, '6252_Car');

-- --------------------------------------------------------

--
-- Table structure for table `violations`
--

CREATE TABLE `violations` (
  `Car_ID` longtext DEFAULT NULL,
  `Start_Gate` varchar(255) DEFAULT NULL,
  `End_Gate` varchar(255) DEFAULT NULL,
  `Start_Date` longtext DEFAULT NULL,
  `Arrival_End_Date` longtext DEFAULT NULL,
  `payment_status` enum('paid','unpaid') DEFAULT 'unpaid'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `violations`
--

INSERT INTO `violations` (`Car_ID`, `Start_Gate`, `End_Gate`, `Start_Date`, `Arrival_End_Date`, `payment_status`) VALUES
('3_Car', 'Giza', 'Qalyubia', '2024-03-11 15:34:01', '2024-03-11 15:35:53', 'unpaid');

--
-- Indexes for dumped tables
--

--
-- Indexes for table `admins`
--
ALTER TABLE `admins`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `drivers`
--
ALTER TABLE `drivers`
  ADD PRIMARY KEY (`ID`);

--
-- Indexes for table `travels`
--
ALTER TABLE `travels`
  ADD KEY `travels_vehicles` (`Vehicle_id`);

--
-- Indexes for table `vehicles`
--
ALTER TABLE `vehicles`
  ADD PRIMARY KEY (`id`),
  ADD KEY `driver_vehicle` (`driver_id`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `admins`
--
ALTER TABLE `admins`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=4;

--
-- AUTO_INCREMENT for table `drivers`
--
ALTER TABLE `drivers`
  MODIFY `ID` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=5;

--
-- AUTO_INCREMENT for table `vehicles`
--
ALTER TABLE `vehicles`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=10;

--
-- Constraints for dumped tables
--

--
-- Constraints for table `travels`
--
ALTER TABLE `travels`
  ADD CONSTRAINT `travels_vehicles` FOREIGN KEY (`Vehicle_id`) REFERENCES `vehicles` (`id`);

--
-- Constraints for table `vehicles`
--
ALTER TABLE `vehicles`
  ADD CONSTRAINT `driver_vehicle` FOREIGN KEY (`driver_id`) REFERENCES `drivers` (`ID`);
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
